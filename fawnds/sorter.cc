/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "sorter.h"
#include "debug.h"
#include "configuration.h"
#ifndef HAVE_LIBNSORT
#include <algorithm>
#endif
#include <cassert>

namespace fawn {

    Sorter::Sorter()
        : open_(false)
    {
    }

    Sorter::~Sorter()
    {
        if (open_)
            Close();
    }

    FawnDS_Return
    Sorter::Create()
    {
        if (open_)
            return ERROR;

        if (config_->ExistsNode("child::key-len") == 0)
            key_len_ = atoi(config_->GetStringValue("child::key-len").c_str());
        else
            key_len_ = 0;

        if (config_->ExistsNode("child::data-len") == 0)
            data_len_ = atoi(config_->GetStringValue("child::data-len").c_str());
        else
            data_len_ = 0;

        if (config_->ExistsNode("child::temp-file") == 0)
            temp_file_ = config_->GetStringValue("child::temp-file");
        else
            temp_file_ = "/tmp";

#ifdef HAVE_LIBNSORT
        char buf[1024];

        if (key_len_ != 0 && data_len_ != 0) {
            const char* nsort_definition =
                " -format=size: %zu"
                " -field=Key, char, offset: 0, size: %zu"
                " -key=Key"
                " -temp_file=%s";

            snprintf(buf, sizeof(buf), nsort_definition, key_len_ + data_len_, key_len_, temp_file_.c_str());
        }
        else {
            // currently not supported
            return ERROR;
        }

        nsort_msg_t ret = nsort_define(buf, 0, NULL, &nsort_ctx_);
        if (ret != NSORT_SUCCESS) {
            fprintf(stderr, "Nsort error: %s\n", nsort_message(&nsort_ctx_));
            return ERROR;
        }

        nsort_buf_.resize(key_len_ + data_len_);
#endif

        open_ = true;
        input_ended_ = false;

        return OK;
    }

#ifndef HAVE_LIBNSORT
    struct _ordering {
        const std::vector<Value>& key_array;

        _ordering(const std::vector<Value>& key_array) : key_array(key_array) {}

        bool operator()(const size_t& i, const size_t& j) {
            // sort key = (entry key, index)
            int cmp = key_array[i].compare(key_array[j]);
            if (cmp != 0)
                return cmp < 0;
            else
                return i < j;   // stable sorting
        };
    };
#endif

    FawnDS_Return
    Sorter::Flush()
    {
        if (!open_)
            return ERROR;
        if (input_ended_)
            return ERROR;

        input_ended_ = true;

#ifndef HAVE_LIBNSORT
        refs_.reserve(key_array_.size());
        for (size_t i = 0; i < key_array_.size(); i++)
            refs_.push_back(i);

        // uses unstable sorting with stable sort key comparison
        // because std::sort() is often more efficient than std::stable_sort()
        std::sort(refs_.begin(), refs_.end(), _ordering(key_array_));

        refs_it_ = refs_.begin();
#else
        nsort_msg_t ret = nsort_release_end(&nsort_ctx_);
        if (ret != NSORT_SUCCESS)
            fprintf(stderr, "Nsort error: %s\n", nsort_message(&nsort_ctx_));
#endif

        return OK;
    }

    FawnDS_Return
    Sorter::Close()
    {
        if (!open_)
            return ERROR;

#ifndef HAVE_LIBNSORT
        key_array_.clear();
        data_array_.clear();
        refs_.clear();
#else
        nsort_end(&nsort_ctx_);
        nsort_buf_.resize(0);
#endif

        open_ = false;
        return OK;
    }

    FawnDS_Return
    Sorter::Put(const ConstValue& key, const ConstValue& data)
    {
        if (!open_)
            return ERROR;
        if (input_ended_)
            return ERROR;

        if (key.size() != key_len_)
            return INVALID_KEY;
        if (data.size() != data_len_)
            return INVALID_DATA;

#ifndef HAVE_LIBNSORT
        NewValue copied_key(key.data(), key.size());
        NewValue copied_value(data.data(), data.size());
        key_array_.push_back(copied_key);
        data_array_.push_back(copied_value);
#else
        memcpy(nsort_buf_.data(), key.data(), key_len_);
        memcpy(nsort_buf_.data() + key_len_, data.data(), data_len_);

        nsort_msg_t ret = nsort_release_recs(nsort_buf_.data(), key_len_ + data_len_, &nsort_ctx_);
        if (ret != NSORT_SUCCESS)
            fprintf(stderr, "Nsort error: %s\n", nsort_message(&nsort_ctx_));
#endif

        return OK;
    }

    FawnDS_ConstIterator
    Sorter::Enumerate() const
    {
        if (!open_)
            return FawnDS_ConstIterator();
        if (!input_ended_)
            return FawnDS_ConstIterator();

        IteratorElem* elem = new IteratorElem(this);
        elem->state = OK;
        elem->Increment(true);

        return FawnDS_ConstIterator(elem);
    }

    FawnDS_Iterator
    Sorter::Enumerate()
    {
        if (!open_)
            return FawnDS_Iterator();
        if (!input_ended_)
            return FawnDS_Iterator();

        IteratorElem* elem = new IteratorElem(this);
        elem->state = OK;
        elem->Increment(true);

        return FawnDS_Iterator(elem);
    }

    Sorter::IteratorElem::IteratorElem(const Sorter* fawnds)
    {
        this->fawnds = fawnds;
    }

    FawnDS_IteratorElem*
    Sorter::IteratorElem::Clone() const
    {
        IteratorElem* elem = new IteratorElem(static_cast<const Sorter*>(fawnds));
        *elem = *this;
        return elem;
    }

    void
    Sorter::IteratorElem::Next()
    {
        Increment(false);
    }

    void
    Sorter::IteratorElem::Increment(bool initial)
    {
        if (state == END)
            return;

        const Sorter* sorter = static_cast<const Sorter*>(fawnds);

#ifndef HAVE_LIBNSORT

        std::vector<size_t>::const_iterator& refs_it = sorter->refs_it_;

        if (!initial)
            ++refs_it;

        if (refs_it == sorter->refs_.end()) {
            state = END;
            return;
        }

        state = OK;
        key = sorter->key_array_[*refs_it];
        data = sorter->data_array_[*refs_it];

#else

        (void)initial;

        size_t size = sorter->key_len_ + sorter->data_len_;
        nsort_msg_t ret = nsort_return_recs(sorter->nsort_buf_.data(), &size, &sorter->nsort_ctx_);
        if (ret == NSORT_SUCCESS) {
            assert(size == sorter->key_len_ + sorter->data_len_);
            key = NewValue(sorter->nsort_buf_.data(), sorter->key_len_);
            data = NewValue(sorter->nsort_buf_.data() + sorter->key_len_, sorter->data_len_);
            state = OK;
        }
        else if (ret == NSORT_END_OF_OUTPUT)
            state = END;
        else {
            fprintf(stderr, "Nsort error: %s\n", nsort_message(&sorter->nsort_ctx_));
            assert(false);
            state = END;
        }

#endif
    }

} // namespace fawn

