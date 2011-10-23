/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "configuration.h"
#include "debug.h"
#include "file_io.h"

#include <xercesc/util/PlatformUtils.hpp>

#include <xercesc/dom/DOM.hpp>

#include <xercesc/framework/StdOutFormatTarget.hpp>
#include <xercesc/framework/LocalFileFormatTarget.hpp>
#include <xercesc/parsers/XercesDOMParser.hpp>
#include <xercesc/util/XMLUni.hpp>
#include <xercesc/util/XMLString.hpp>

#include <xercesc/util/OutOfMemoryException.hpp>

#include <memory>
#include <libgen.h>

#include "DOMTreeErrorReporter.hpp"
#include "DOMWriteErrorHandler.hpp"

using namespace std;

namespace fawn {

    class ConfigurationMutex {
    public:
        static ConfigurationMutex* getInstance();
        pthread_mutex_t mutex_;
    private:
        ConfigurationMutex();
        static std::auto_ptr<ConfigurationMutex> instance;
    };

    class ConfigurationObject {
    public:
        ConfigurationObject();
        ~ConfigurationObject();
        DOMDocument* doc_;
        int instance_counter_;
        mutable pthread_mutex_t config_mutex_;
    };


    ConfigurationMutex::ConfigurationMutex() {
        pthread_mutex_init(&mutex_, NULL);
    }

    std::auto_ptr<ConfigurationMutex> ConfigurationMutex::instance(NULL);
    ConfigurationMutex* ConfigurationMutex::getInstance() {
        if (instance.get() == NULL) {
            instance.reset(new ConfigurationMutex());
        }
        return instance.get();
    }

    ConfigurationObject::ConfigurationObject()
    {
        instance_counter_ = 1;
        pthread_mutex_init(&config_mutex_, NULL);
    }

    ConfigurationObject::~ConfigurationObject()
    {
        assert(instance_counter_ == 0);
        pthread_mutex_destroy(&config_mutex_);
        delete doc_;
    }

    tbb::queuing_mutex Configuration::mutex_;

    Configuration::Configuration()
    {
        DPRINTF(2, "Configuration: Called constructor without config file\n");
        initialize();
        DPRINTF(2, "Configuration: Constructor: initialized!\n");
        XMLCh* XMLCh_name = XMLString::transcode("core");
        DOMImplementation * DOM_impl = DOMImplementationRegistry::getDOMImplementation(XMLCh_name);
        XMLString::release(&XMLCh_name);
        conf_obj_ = new ConfigurationObject();
        XMLCh_name = XMLString::transcode("fawnds");
        conf_obj_->doc_ = DOM_impl->createDocument(NULL, XMLCh_name, NULL);
        XMLString::release(&XMLCh_name);
        context_node_ = conf_obj_->doc_->getDocumentElement();
        DPRINTF(2, "Configuration: Constructor: created object!\n");
    }

    Configuration::Configuration(const string &config_file)
    {
        DPRINTF(2, "Configuration: Called constructor with config file: %s\n", config_file.c_str());
        initialize();
        DPRINTF(2, "Configuration: Constructor: initialized!\n");
        conf_obj_ = new ConfigurationObject();
        conf_obj_->doc_ = readConfigFile(config_file);
        context_node_ = conf_obj_->doc_->getDocumentElement();

        // handle <include>
        while (ExistsNode("child::include") == 0) {
            char file[config_file.size() + 1024];
            strcpy(file, config_file.c_str());
            
            string subconfig_file = string(dirname(file)) + "/";
            subconfig_file += GetStringValue("child::include/file");

            Configuration* subconfig = new Configuration(subconfig_file);

            string source_gXPathExpression = GetStringValue("child::include/src");
            string dest_gXPathExpression = GetStringValue("child::include/dest");

            DOMXPathResult* source_xpath_result = subconfig->getXPathResult(source_gXPathExpression);
            DOMXPathResult* dest_xpath_result = this->getXPathResult(dest_gXPathExpression);
            bool errorsOccured = false;
            if (source_xpath_result != NULL && dest_xpath_result!= NULL) {
                try {
                    DOMNode* source_node = source_xpath_result->getNodeValue();
                    DOMNode* dest_node = dest_xpath_result->getNodeValue();
                    dest_node->appendChild(conf_obj_->doc_->importNode(source_node, true));
                }
                catch(const DOMXPathException& e) {
                    XERCES_STD_QUALIFIER cerr << "Configuration: CloneAndAppendNode: An error occurred during XML node value retrieval. Msg is:"
                                              << XERCES_STD_QUALIFIER endl
                                              << StrX(e.getMessage()) << XERCES_STD_QUALIFIER endl;
                    errorsOccured = true;
                }

                source_xpath_result->release();
                dest_xpath_result->release();
            } else {
                errorsOccured = true;
            }

            delete subconfig;
            DeleteNode("child::include");
        }

        // handle <set>
        while (ExistsNode("child::set") == 0) {
            string node = GetStringValue("child::set/node");
            string value = GetStringValue("child::set/value");
            SetStringValue(node, value);
            DeleteNode("child::set");
        }
        DPRINTF(2, "Configuration: Constructor: created object!\n");
    }

    Configuration::Configuration(const Configuration* const &other, bool copy)
    {
        if (!copy) {
            DPRINTF(2, "Configuration: Called constructor with config object\n");
            initialize();
            DPRINTF(2, "Configuration: Constructor: initialized!\n");
            pthread_mutex_lock(&(ConfigurationMutex::getInstance()->mutex_));
            ++(other->conf_obj_->instance_counter_);
            conf_obj_ = other->conf_obj_;
            pthread_mutex_unlock(&(ConfigurationMutex::getInstance()->mutex_));
            context_node_ = other->context_node_;
            DPRINTF(2, "Configuration: Constructor: created object!\n");
        }
        else 
        {
            DPRINTF(2, "Configuration: Called constructor with config object\n");
            initialize();
            DPRINTF(2, "Configuration: Constructor: initialized!\n");
            conf_obj_ = new ConfigurationObject();
            DOMImplementation* impl = DOMImplementation::getImplementation();
            assert(impl);
            conf_obj_->doc_ = impl->createDocument(); // non-standard extension of Xerces-C++
            DOMNode* imported_node = conf_obj_->doc_->importNode(other->context_node_, true);
            assert(imported_node);
            conf_obj_->doc_->appendChild(imported_node);
            context_node_ = conf_obj_->doc_->getDocumentElement();
            DPRINTF(2, "Configuration: Constructor: created object!\n");
        }
    }

    Configuration::~Configuration()
    {
        pthread_mutex_lock(&(ConfigurationMutex::getInstance()->mutex_));
        --(conf_obj_->instance_counter_);
        // TODO: this may have a race condition that deletes conf_obj_ while instance_counter_ was about to increase in the constructor.
        // fixing this may require a weak pointer
        if (conf_obj_->instance_counter_ <= 0) {
            delete conf_obj_;
        }
        terminate();
        pthread_mutex_unlock(&(ConfigurationMutex::getInstance()->mutex_));
    }

    int
    Configuration::CloneAndAppendNode(const string& source_gXPathExpression, const string& dest_gXPathExpression)
    {
        pthread_mutex_lock(&(conf_obj_->config_mutex_));
        DPRINTF(2, "Configuration: Called CloneAndAppendNode: source: %s dest: %s\n", source_gXPathExpression.c_str(), dest_gXPathExpression.c_str());
        DOMXPathResult* source_xpath_result = Configuration::getXPathResult(source_gXPathExpression);
        DOMXPathResult* dest_xpath_result = Configuration::getXPathResult(dest_gXPathExpression);
        bool errorsOccured = false;
        if (source_xpath_result != NULL && dest_xpath_result!= NULL) {
            try {
                DOMNode* source_node = source_xpath_result->getNodeValue();
                DOMNode* dest_node = dest_xpath_result->getNodeValue();
                dest_node->appendChild(source_node->cloneNode(true));
            }
            catch(const DOMXPathException& e) {
                XERCES_STD_QUALIFIER cerr << "Configuration: CloneAndAppendNode: An error occurred during XML node value retrieval. Msg is:"
                                          << XERCES_STD_QUALIFIER endl
                                          << StrX(e.getMessage()) << XERCES_STD_QUALIFIER endl;
                errorsOccured = true;
            }

            source_xpath_result->release();
            dest_xpath_result->release();
        } else {
            errorsOccured = true;
        }

        if(errorsOccured) {
            pthread_mutex_unlock(&(conf_obj_->config_mutex_));
            return -1;
        }

        pthread_mutex_unlock(&(conf_obj_->config_mutex_));
        return 0;
    }

    Configuration*
    Configuration::CloneAndAppendNodeAndGetConfig(const string& source_gXPathExpression, const string& dest_gXPathExpression)
    {
        pthread_mutex_lock(&(conf_obj_->config_mutex_));
        DPRINTF(2, "Configuration: Called CloneAndAppendNodeAndGetConfig: source: %s dest: %s\n", source_gXPathExpression.c_str(), dest_gXPathExpression.c_str());
        DOMXPathResult* source_xpath_result = Configuration::getXPathResult(source_gXPathExpression);
        DOMXPathResult* dest_xpath_result = Configuration::getXPathResult(dest_gXPathExpression);
        bool errorsOccured = false;
        Configuration* ret = NULL;
        if (source_xpath_result != NULL && dest_xpath_result!= NULL) {
            try {
                DOMNode* source_node = source_xpath_result->getNodeValue();
                DOMNode* dest_node = dest_xpath_result->getNodeValue();
                DOMNode* clone_node = source_node->cloneNode(true);
                if (clone_node->getNodeType() != DOMNode::ELEMENT_NODE) {
                    errorsOccured = true;
                } else {
                    dest_node->appendChild(clone_node);
                    ret = new Configuration(this);
                    ret->SetContextNode((DOMElement*)clone_node);
                }
            }
            catch(const DOMXPathException& e) {
                XERCES_STD_QUALIFIER cerr << "Configuration: CloneAndAppendNode: An error occurred during XML node value retrieval. Msg is:"
                                          << XERCES_STD_QUALIFIER endl
                                          << StrX(e.getMessage()) << XERCES_STD_QUALIFIER endl;
                errorsOccured = true;
            }

            source_xpath_result->release();
            dest_xpath_result->release();
        } else {
            errorsOccured = true;
        }

        if(errorsOccured) {
            pthread_mutex_unlock(&(conf_obj_->config_mutex_));
            return NULL;
        }

        pthread_mutex_unlock(&(conf_obj_->config_mutex_));
        return ret;
    }

    int
    Configuration::CloneAndReplaceNode(const string& source_gXPathExpression, const string& dest_gXPathExpression)
    {
        pthread_mutex_lock(&(conf_obj_->config_mutex_));
        DPRINTF(2, "Configuration: Called CloneAndReplaceNode: source: %s dest: %s\n", source_gXPathExpression.c_str(), dest_gXPathExpression.c_str());
        DOMXPathResult* source_xpath_result = Configuration::getXPathResult(source_gXPathExpression);
        DOMXPathResult* dest_xpath_result = Configuration::getXPathResult(dest_gXPathExpression);
        bool errorsOccured = false;
        if (source_xpath_result != NULL && dest_xpath_result!= NULL) {
            try {
                DOMNode* source_node = source_xpath_result->getNodeValue();
                DOMNode* dest_node = dest_xpath_result->getNodeValue();
                dest_node->getParentNode()->replaceChild(source_node->cloneNode(true), dest_node)->release();
            }
            catch(const DOMXPathException& e) {
                XERCES_STD_QUALIFIER cerr << "Configuration: CloneAndReplaceNode: An error occurred during XML node value retrieval. Msg is:"
                                          << XERCES_STD_QUALIFIER endl
                                          << StrX(e.getMessage()) << XERCES_STD_QUALIFIER endl;
                errorsOccured = true;
            }

            source_xpath_result->release();
            dest_xpath_result->release();
        } else {
            errorsOccured = true;
        }

        if(errorsOccured) {
            pthread_mutex_unlock(&(conf_obj_->config_mutex_));
            return -1;
        }

        pthread_mutex_unlock(&(conf_obj_->config_mutex_));
        return 0;
    }

    int
    Configuration::CreateNodeAndAppend(const string& tagName, const string& dest_gXPathExpression)
    {
        pthread_mutex_lock(&(conf_obj_->config_mutex_));
        DPRINTF(2, "Configuration: Called CreateNodeAndAppend with path: %s and tagName: %s\n", dest_gXPathExpression.c_str(), tagName.c_str());
        DOMXPathResult* xpath_result = Configuration::getXPathResult(dest_gXPathExpression);
        bool errorsOccured = false;
        if (xpath_result != NULL) {
            try {
                DOMNode* node = xpath_result->getNodeValue();
                XMLCh* XMLCh_name = XMLString::transcode(tagName.c_str());
                DOMNode* new_node = conf_obj_->doc_->createElement(XMLCh_name);
                XMLString::release(&XMLCh_name);
                node->appendChild(new_node);
            }
            catch(const DOMXPathException& e) {
                XERCES_STD_QUALIFIER cerr << "Configuration: CreateNodeAndAppend: An error occurred during XML node value retrieval. Msg is:"
                                          << XERCES_STD_QUALIFIER endl
                                          << StrX(e.getMessage()) << XERCES_STD_QUALIFIER endl;
                errorsOccured = true;
            }

            xpath_result->release();
        } else {
            errorsOccured = true;
        }

        if(errorsOccured) {
            pthread_mutex_unlock(&(conf_obj_->config_mutex_));
            return -1;
        }

        pthread_mutex_unlock(&(conf_obj_->config_mutex_));
            return 0;
    }

    int
    Configuration::DeleteNode(const string& gXPathExpression)
    {
        pthread_mutex_lock(&(conf_obj_->config_mutex_));
        DPRINTF(2, "Configuration: Called DeleteNode with path: %s\n", gXPathExpression.c_str());
        DOMXPathResult* xpath_result = Configuration::getXPathResult(gXPathExpression);
        bool errorsOccured = false;
        if (xpath_result != NULL) {
            try {
                DOMNode* node = xpath_result->getNodeValue();
                if (node != NULL) {
                    node->getParentNode()->removeChild(node)->release();
                } else {
                    errorsOccured = true;
                }
            }
            catch(const DOMXPathException& e) {
                XERCES_STD_QUALIFIER cerr << "Configuration: DeleteNode: An error occurred during XML node value retrieval. Msg is:"
                                          << XERCES_STD_QUALIFIER endl
                                          << StrX(e.getMessage()) << XERCES_STD_QUALIFIER endl;
                errorsOccured = true;
            }
        } else {
            errorsOccured = true;
        }

        xpath_result->release();

        if(errorsOccured) {
            pthread_mutex_unlock(&(conf_obj_->config_mutex_));
            return -1;
        }

        pthread_mutex_unlock(&(conf_obj_->config_mutex_));
        return 0;
    }

    int
    Configuration::ExistsNode(const string& gXPathExpression) const
    {
        pthread_mutex_lock(&(conf_obj_->config_mutex_));
        DPRINTF(2, "Configuration: Called ExistsNode with path: %s\n", gXPathExpression.c_str());
        DOMXPathResult* xpath_result = Configuration::getXPathResult(gXPathExpression);
        bool errorsOccured = false;
        if (xpath_result != NULL) {
            try {
                DOMNode* node = xpath_result->getNodeValue();
                if (node == NULL) {
                    errorsOccured = true;
                }
            }
            catch(const DOMXPathException& e) {
                XERCES_STD_QUALIFIER cerr << "Configuration: ExistsNode: An error occurred during XML node value retrieval. Msg is:"
                                          << XERCES_STD_QUALIFIER endl
                                          << StrX(e.getMessage()) << XERCES_STD_QUALIFIER endl;
                errorsOccured = true;
            }
        } else {
            errorsOccured = true;
        }

        xpath_result->release();

        if(errorsOccured) {
            pthread_mutex_unlock(&(conf_obj_->config_mutex_));
            return -1;
        }

        pthread_mutex_unlock(&(conf_obj_->config_mutex_));
        return 0;

    }

    const string
    Configuration::GetStringValue(const string& gXPathExpression) const
    {
        pthread_mutex_lock(&(conf_obj_->config_mutex_));
        DPRINTF(2, "Configuration: Called getStringValue with path: %s\n", gXPathExpression.c_str());
        bool errorsOccured = false;
        string result;
        DOMXPathResult* xpath_result = Configuration::getXPathResult(gXPathExpression);
        if (xpath_result != NULL) {
            try {
                DOMNode* node = xpath_result->getNodeValue();
                if (node == NULL) {
                    errorsOccured = true;
                } else {
                    const XMLCh* XMLCh_result = node->getTextContent();
                    char* result_c = XMLString::transcode(XMLCh_result);
                    result.append(result_c);
                    XMLString::release(&result_c);
                }
            }
            catch(const DOMXPathException& e) {
                XERCES_STD_QUALIFIER cerr << "Configuration: GetStringValue: An error occurred during XML node value retrieval. Msg is:"
                                          << XERCES_STD_QUALIFIER endl
                                          << StrX(e.getMessage()) << XERCES_STD_QUALIFIER endl;
                errorsOccured = true;
            }

            xpath_result->release();
        } else
            errorsOccured = true;

        DPRINTF(2, "Configuration: getStringValue: result: %s\n", tmp.c_str());
        pthread_mutex_unlock(&(conf_obj_->config_mutex_));
        return result;
    }

    int
    Configuration::SetStringValue(const string& gXPathExpression, const string& value)
    {
        pthread_mutex_lock(&(conf_obj_->config_mutex_));
        DPRINTF(2, "Configuration: Called setStringValue with path: %s and value: %s\n", gXPathExpression.c_str(), value.c_str());
        DOMXPathResult* xpath_result = Configuration::getXPathResult(gXPathExpression);
        bool errorsOccured = false;
        try {
                DOMNode* node = xpath_result->getNodeValue();
                if (!node) {
                    XERCES_STD_QUALIFIER cerr << "Configuration: SetStringValue: Node for " << gXPathExpression << " not found"
                                              << XERCES_STD_QUALIFIER endl;
                    errorsOccured = true;
                }
                XMLCh* XMLCh_value = XMLString::transcode(value.c_str());
                node->setTextContent(XMLCh_value);
                XMLString::release(&XMLCh_value);
            }
        catch(const DOMXPathException& e) {
                XERCES_STD_QUALIFIER cerr << "Configuration: SetStringValue: An error occurred during XML node value retrieval. Msg is:"
                                          << XERCES_STD_QUALIFIER endl
                                          << StrX(e.getMessage()) << XERCES_STD_QUALIFIER endl;
                errorsOccured = true;
            }

        xpath_result->release();

        if(errorsOccured) {
            pthread_mutex_unlock(&(conf_obj_->config_mutex_));
            return -1;
        }

        pthread_mutex_unlock(&(conf_obj_->config_mutex_));
        return 0;
    }

    int
    Configuration::WriteConfigFile(const std::string& config_file) const
    {
        pthread_mutex_lock(&(conf_obj_->config_mutex_));
        bool errorsOccured = false;
        try {
                // get a serializer, an instance of DOMLSSerializer
                XMLCh tempStr[3] = {chLatin_L, chLatin_S, chNull};
                DOMImplementation *impl          = DOMImplementationRegistry::getDOMImplementation(tempStr);
                DOMLSSerializer   *theSerializer = ((DOMImplementationLS*)impl)->createLSSerializer();
                DOMLSOutput       *theOutputDesc = ((DOMImplementationLS*)impl)->createLSOutput();

                // plug in user's own error handler
                DOMErrorHandler *myErrorHandler = new DOMWriteErrorHandler();
                DOMConfiguration* serializerConfig = theSerializer->getDomConfig();
                serializerConfig->setParameter(XMLUni::fgDOMErrorHandler, myErrorHandler);

                // // set feature if the serializer supports the feature/mode
                // if (serializerConfig->canSetParameter(XMLUni::fgDOMWRTSplitCdataSections, gSplitCdataSections))
                //     serializerConfig->setParameter(XMLUni::fgDOMWRTSplitCdataSections, gSplitCdataSections);

                // if (serializerConfig->canSetParameter(XMLUni::fgDOMWRTDiscardDefaultContent, gDiscardDefaultContent))
                //     serializerConfig->setParameter(XMLUni::fgDOMWRTDiscardDefaultContent, gDiscardDefaultContent);

                // if (serializerConfig->canSetParameter(XMLUni::fgDOMWRTFormatPrettyPrint, gFormatPrettyPrint))
                //     serializerConfig->setParameter(XMLUni::fgDOMWRTFormatPrettyPrint, gFormatPrettyPrint);

                // if (serializerConfig->canSetParameter(XMLUni::fgDOMWRTBOM, gWriteBOM))
                //     serializerConfig->setParameter(XMLUni::fgDOMWRTBOM, gWriteBOM);

                //
                // Plug in a format target to receive the resultant
                // XML stream from the serializer.
                //
                // StdOutFormatTarget prints the resultant XML stream
                // to stdout once it receives any thing from the serializer.
                //
                XMLFormatTarget *myFormTarget;
                myFormTarget = new LocalFileFormatTarget(config_file.c_str());
                theOutputDesc->setByteStream(myFormTarget);

                theSerializer->write(conf_obj_->doc_, theOutputDesc);

                theOutputDesc->release();
                theSerializer->release();

                //
                // Filter, formatTarget and error handler
                // are NOT owned by the serializer.
                //
                if(myFormTarget)
                    delete myFormTarget;
                if(myErrorHandler)
                    delete myErrorHandler;

                // if (gUseFilter)
                //     delete myFilter;

            }
        catch (const OutOfMemoryException&) {
                XERCES_STD_QUALIFIER cerr << "Configuration: WriteConfigFile: OutOfMemoryException" << XERCES_STD_QUALIFIER endl;
                errorsOccured = true;
            }
        catch (XMLException& e) {
                XERCES_STD_QUALIFIER cerr << "Configuration: WriteConfigFile: An error occurred during creation of output transcoder. Msg is:"
                                          << XERCES_STD_QUALIFIER endl
                                          << StrX(e.getMessage()) << XERCES_STD_QUALIFIER endl;
                errorsOccured = true;
            }

        if(errorsOccured) {
                pthread_mutex_unlock(&(conf_obj_->config_mutex_));
                return -1;
            }
        pthread_mutex_unlock(&(conf_obj_->config_mutex_));
        return 0;
    }

    int
    Configuration::SetContextNode(const string& gXPathExpression)
    {
        pthread_mutex_lock(&(conf_obj_->config_mutex_));
        DPRINTF(2, "Configuration: Called SetContextNode with path: %s\n", gXPathExpression.c_str());
        DOMXPathResult* xpath_result = Configuration::getXPathResult(gXPathExpression);
        bool errorsOccured = false;
        if (xpath_result != NULL) {
            try {
                DPRINTF(2, "Configuration: SetContextNode: test result isNode\n");
                if (xpath_result->getResultType() == DOMXPathResult::FIRST_ORDERED_NODE_TYPE) {
                    DPRINTF(2, "Configuration: SetContextNode: try to retrieve node from result\n");
                    DOMNode* node = xpath_result->getNodeValue();
                    if (node != NULL) {
                        if (node->getNodeType() != DOMNode::ELEMENT_NODE) {
                            DPRINTF(2, "Configuration: SetContextNode: result node was not of type DOMElement (=1) but of type: %i\n", node->getNodeType());
                            errorsOccured = true;
                        } else {
                            DPRINTF(2, "Configuration: SetContextNode: node value inside result is null or not of correct type\n");
                            context_node_ = (DOMElement*)node;
                        }
                    } else {
                    DPRINTF(2, "Configuration: SetContextNode: result node is null (no node found)\n");
                    errorsOccured = true;
                    }
                } else {
                    DPRINTF(2, "Configuration: SetContextNode: result not of type first_ordered_node_type\n");
                    errorsOccured = true;
                }
            }
            catch(const DOMXPathException& e) {
                XERCES_STD_QUALIFIER cerr << "Configuration: SetContextNode: An error occurred during XML node value retrieval. Msg is:"
                                          << XERCES_STD_QUALIFIER endl
                                          << StrX(e.getMessage()) << XERCES_STD_QUALIFIER endl;
                errorsOccured = true;
            }

            xpath_result->release();
        } else {
            DPRINTF(2, "Configuration: SetContextNode: xPathResult is null\n");
            errorsOccured = true;
        }
        if(errorsOccured) {
            DPRINTF(2, "Configuration: SetContextNode: errors occured\n");
            pthread_mutex_unlock(&(conf_obj_->config_mutex_));
            return -1;
        }

        pthread_mutex_unlock(&(conf_obj_->config_mutex_));
        return 0;
    }

    void
    Configuration::ResetContextNode()
    {
        pthread_mutex_lock(&(conf_obj_->config_mutex_));
        context_node_ = conf_obj_->doc_->getDocumentElement();
        pthread_mutex_unlock(&(conf_obj_->config_mutex_));
    }


    DOMDocument*
    Configuration::readConfigFile(const string& config_file)
    {
        //
        //  Create our parser, then attach an error handler to the parser.
        //  The parser will call back to methods of the ErrorHandler if it
        //  discovers errors during the course of parsing the XML document.
        //
        XercesDOMParser *parser = new XercesDOMParser;
        // parser->setValidationScheme(gValScheme);
        // parser->setDoNamespaces(gDoNamespaces);
        // parser->setDoSchema(gDoSchema);
        // parser->setHandleMultipleImports (true);
        // parser->setValidationSchemaFullChecking(gSchemaFullChecking);
        // parser->setCreateEntityReferenceNodes(gDoCreate);

        DOMTreeErrorReporter *errReporter = new DOMTreeErrorReporter();
        parser->setErrorHandler(errReporter);

        //
        //  Parse the XML file, catching any XML exceptions that might propogate
        //  out of it.
        //
        bool errorsOccured = false;
        try {
            parser->parse(config_file.c_str());
        }
        catch (const OutOfMemoryException&) {
            XERCES_STD_QUALIFIER cerr << "Configuration: readConfigFile: OutOfMemoryException" << XERCES_STD_QUALIFIER endl;
            errorsOccured = true;
        }
        catch (const XMLException& e) {
            XERCES_STD_QUALIFIER cerr << "Configuration: readConfigFile: An error occurred during parsing\n   Message: "
                                      << StrX(e.getMessage()) << XERCES_STD_QUALIFIER endl;
            errorsOccured = true;
        }
        catch (const DOMException& e) {
            const unsigned int maxChars = 2047;
            XMLCh errText[maxChars + 1];

            XERCES_STD_QUALIFIER cerr << "\nConfiguration: readConfigFile: DOM Error during parsing: '" << config_file << "'\n"
                                      << "DOMException code is:  " << e.code << XERCES_STD_QUALIFIER endl;

            if (DOMImplementation::loadDOMExceptionMsg(e.code, errText, maxChars))
                XERCES_STD_QUALIFIER cerr << "Message is: " << StrX(errText) << XERCES_STD_QUALIFIER endl;

            errorsOccured = true;
        }

        catch (...) {
            XERCES_STD_QUALIFIER cerr << "Configuration: readConfigFile: An error occurred during parsing\n " << XERCES_STD_QUALIFIER endl;
            errorsOccured = true;
        }

        DOMDocument *doc = NULL;

        // If the parse was successful, output the document data from the DOM tree
        if (!errorsOccured && !errReporter->getSawErrors()) {
            // get the DOM representation
            doc = parser->getDocument();
            parser->adoptDocument();
        }

        //
        //  Clean up the error handler. The parser does not adopt handlers
        //  since they could be many objects or one object installed for multiple
        //  handlers.
        //
        if (errReporter)
            delete errReporter;

        //
        //  Delete the parser itself.  Must be done prior to calling Configuration::Terminate, below.
        //
        if (parser)
            delete parser;

        return doc;
    }

    DOMXPathResult*
    Configuration::getXPathResult(const string& gXPathExpression) const
    {
        bool errorsOccured = false;
        DOMXPathResult* result = NULL;
        if(gXPathExpression != "")
            {
                XMLCh* xpathStr=XMLString::transcode(gXPathExpression.c_str());
                try {
                    result = conf_obj_->doc_->evaluate(xpathStr,
                                                       context_node_,
                                                       NULL,
                                                       DOMXPathResult::FIRST_ORDERED_NODE_TYPE,
                                                       NULL);
                }
                catch(const DOMXPathException& e) {
                    XERCES_STD_QUALIFIER cerr << "Configuration: getXPathResult: An error occurred during processing of the XPath expression. Msg is:"
                                              << XERCES_STD_QUALIFIER endl
                                              << StrX(e.getMessage()) << XERCES_STD_QUALIFIER endl;
                    errorsOccured = true;
                }
                catch(const DOMException& e) {
                    XERCES_STD_QUALIFIER cerr << "Configuration: getXPathResult: An error occurred during processing of the XPath expression. Msg is:"
                                              << XERCES_STD_QUALIFIER endl
                                              << StrX(e.getMessage()) << XERCES_STD_QUALIFIER endl;
                    errorsOccured = true;
                }
                XMLString::release(&xpathStr);
            }
        else
            errorsOccured = true;

        if(errorsOccured) {
            DPRINTF(2, "Configuration: getXPathResult: errors occured\n");
            return NULL;
        }

        return result;
    }

    void
    Configuration::SetContextNode(DOMElement* node) {
        context_node_ = node;
    }

    void Configuration::initialize()
    {
        // Configuration::Initialize the XML4C2 system
        // this requires locking according to http://xerces.apache.org/xerces-c/faq-parse-3.html#faq-6

        tbb::queuing_mutex::scoped_lock lock(mutex_);

        try {
                XMLPlatformUtils::Initialize();
            }

        catch(const XMLException &toCatch) {
                XERCES_STD_QUALIFIER cerr << "Configuration: initialize: Error during Xerces-c Initialization.\n"
                                          << "  Exception message:"
                                          << StrX(toCatch.getMessage()) << XERCES_STD_QUALIFIER endl;
                exit(-1);
            }
    }

    void Configuration::terminate()
    {
        tbb::queuing_mutex::scoped_lock lock(mutex_);

        // And call the termination method
        XMLPlatformUtils::Terminate();
    }

} // namespace fawn
