/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _FAWNDS_ITERATOR_H_
#define _FAWNDS_ITERATOR_H_

#include "basic_types.h"
#include "fawnds_types.h"
#include "value.h"
#include <iterator>

namespace fawn {

    class FawnDS;

    struct FawnDS_IteratorElem {
        FawnDS_Return state;

        Value key;
        Value data;

        const FawnDS* fawnds;

        FawnDS_IteratorElem();
        virtual ~FawnDS_IteratorElem();

        virtual FawnDS_IteratorElem* Clone() const;

        virtual void Prev();
        virtual void Next();

        // the following methods may const_cast<FawnDS*>(fawnds)
        // because these are exposed as non-const to the user only by FawnDS_Iterator
        virtual FawnDS_Return Replace(const ConstValue& data);
        virtual FawnDS_Return Replace(const ConstValue& key, const ConstValue& data);
        virtual FawnDS_Return Delete();
    };

    struct FawnDS_ConstIterator : public std::iterator<std::bidirectional_iterator_tag, FawnDS_IteratorElem> {
        FawnDS_IteratorElem* elem;

        FawnDS_ConstIterator();
        ~FawnDS_ConstIterator();

        FawnDS_ConstIterator(FawnDS_IteratorElem* elem); // move
        FawnDS_ConstIterator(const FawnDS_ConstIterator& rhs);
        FawnDS_ConstIterator& operator=(const FawnDS_ConstIterator& rhs);

        bool IsOK() const;
        bool IsError() const;
        bool IsUnsupported() const;
        bool IsKeyDeleted() const;
        bool IsKeyNotFound() const;
        bool IsInvalidKey() const;
        bool IsInvalidData() const;
        bool IsInvalidLength() const;
        //bool IsUnused() const;
        bool IsEnd() const;

        FawnDS_ConstIterator& operator--();
        FawnDS_ConstIterator operator--(int);
        FawnDS_ConstIterator& operator++();
        FawnDS_ConstIterator operator++(int);

        const FawnDS_IteratorElem& operator*() const;
        const FawnDS_IteratorElem* operator->() const;
    };

    struct FawnDS_Iterator : public FawnDS_ConstIterator {
        FawnDS_Iterator();

        FawnDS_Iterator(FawnDS_IteratorElem* elem); // move
        FawnDS_Iterator(const FawnDS_Iterator& rhs);
        FawnDS_Iterator& operator=(const FawnDS_Iterator& rhs);
        FawnDS_Iterator& operator=(const FawnDS_ConstIterator& rhs);

        FawnDS_Iterator& operator--();
        FawnDS_Iterator operator--(int);
        FawnDS_Iterator& operator++();
        FawnDS_Iterator operator++(int);

        FawnDS_IteratorElem& operator*();
        FawnDS_IteratorElem* operator->();
    };

} // namespace fawn

#endif  // #ifndef _FAWNDS_ITERATOR_H_
