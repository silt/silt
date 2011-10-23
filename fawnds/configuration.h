/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _CONFIGURATION_H_
#define _CONFIGURATION_H_

#include <xercesc/util/XercesDefs.hpp>
#include <string>
#include <tbb/queuing_mutex.h>

XERCES_CPP_NAMESPACE_BEGIN
    class DOMDocument;
    class DOMXPathResult;
    class DOMElement;
XERCES_CPP_NAMESPACE_END

namespace fawn {
    class ConfigurationObject;

    class Configuration {
    public:
        Configuration();
        Configuration(const std::string &config_file);
        Configuration(const Configuration* const &other, bool copy = false);
        ~Configuration();

        int CloneAndAppendNode(const std::string& source_gXPathExpression, const std::string& dest_gXPathExpression);
        Configuration* CloneAndAppendNodeAndGetConfig(const std::string& source_gXPathExpression, const std::string& dest_gXPathExpression);
        int CloneAndReplaceNode(const std::string& source_gXPathExpression, const std::string& dest_gXPathExpression);
        int CreateNodeAndAppend(const std::string& tagName, const std::string& dest_gXPathExpression);
        int DeleteNode(const std::string& gXPathExpression);
        int ExistsNode(const std::string& gXPathExpression) const;
        const std::string GetStringValue(const std::string& gXPathExpression) const;
        int SetStringValue(const std::string& gXPathExpression, const std::string& value);
        int WriteConfigFile(const std::string& config_file) const;
        int SetContextNode(const std::string& gXPathExpression);
        void ResetContextNode();
    private:
        XERCES_CPP_NAMESPACE_QUALIFIER DOMDocument* readConfigFile(const std::string& config_file);
        XERCES_CPP_NAMESPACE_QUALIFIER DOMXPathResult* getXPathResult(const std::string& gXPathExpression) const;

        void SetContextNode(XERCES_CPP_NAMESPACE_QUALIFIER DOMElement* node);

        void initialize();
        void terminate();

        ConfigurationObject* conf_obj_;
        XERCES_CPP_NAMESPACE_QUALIFIER DOMElement* context_node_;

        static tbb::queuing_mutex mutex_;
    };

} // namespace fawn

#endif  // #ifndef _CONFIGURATION_H_
