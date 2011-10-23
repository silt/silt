/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "fawnds_iterator.h"
#include "fawnds.h"
#include "debug.h"

#include <cassert>

namespace fawn {
    class FawnDS;
    class FawnDSIter;

	FawnDS_IteratorElem::FawnDS_IteratorElem()
		: state(END), fawnds(NULL)
	{
	}

	FawnDS_IteratorElem::~FawnDS_IteratorElem()
   	{
   	}

    FawnDS_IteratorElem*
	FawnDS_IteratorElem::Clone() const
	{
	   	return NULL;
   	}

    void
	FawnDS_IteratorElem::Prev()
	{
        state = UNSUPPORTED;
   	}

    void
	FawnDS_IteratorElem::Next()
	{
        state = UNSUPPORTED;
   	}

    FawnDS_Return
    FawnDS_IteratorElem::Replace(const ConstValue& data)
    {
        (void)data;
        return UNSUPPORTED;
    }

    FawnDS_Return
    FawnDS_IteratorElem::Replace(const ConstValue& key, const ConstValue& data)
    {
        (void)key; (void)data;
        return UNSUPPORTED;
    }

    FawnDS_Return
    FawnDS_IteratorElem::Delete()
    {
        return UNSUPPORTED;
    }

	FawnDS_ConstIterator::FawnDS_ConstIterator()
		: elem(NULL)
	{
	}

    FawnDS_ConstIterator::FawnDS_ConstIterator(FawnDS_IteratorElem* elem)
	   	: elem(elem)
   	{
	   	assert(elem);
   	}

	FawnDS_ConstIterator::FawnDS_ConstIterator(const FawnDS_ConstIterator& it)
	   	: elem(NULL)
   	{
	   	*this = it;
   	}

	FawnDS_ConstIterator::~FawnDS_ConstIterator()
   	{
	   	delete elem;
	   	elem = NULL;
   	}

	FawnDS_ConstIterator&
	FawnDS_ConstIterator::operator=(const FawnDS_ConstIterator& rhs)
   	{
	   	delete elem;
	   	if (rhs.elem)
		   	elem = rhs.elem->Clone();
	   	else
		   	elem = NULL;
	   	return *this;
   	}

    bool
    FawnDS_ConstIterator::IsOK() const
    {
        return elem && elem->state == OK;
    }

    bool
    FawnDS_ConstIterator::IsError() const
    {
        return elem && elem->state == ERROR;
    }

    bool
    FawnDS_ConstIterator::IsUnsupported() const
    {
        return elem && elem->state == UNSUPPORTED;
    }

    bool
    FawnDS_ConstIterator::IsKeyDeleted() const
    {
        return elem && elem->state == KEY_DELETED;
    }

    bool
    FawnDS_ConstIterator::IsKeyNotFound() const
    {
        return elem && elem->state == KEY_NOT_FOUND;
    }

    bool
    FawnDS_ConstIterator::IsInvalidKey() const
    {
        return elem && elem->state == INVALID_KEY;
    }

    bool
    FawnDS_ConstIterator::IsInvalidData() const
    {
        return elem && elem->state == INVALID_DATA;
    }

    bool
    FawnDS_ConstIterator::IsInvalidLength() const
    {
        return elem && elem->state == INVALID_LENGTH;
    }

    /*
    bool
    FawnDS_ConstIterator::IsUnused() const
    {
        return elem && elem->state == UNUSED;
    }
    */

    bool
    FawnDS_ConstIterator::IsEnd() const
    {
        return !elem || elem->state == END;
    }

	FawnDS_ConstIterator&
	FawnDS_ConstIterator::operator--()
   	{
	   	assert(elem);
	   	elem->Prev();
	   	return *this;
   	}

	FawnDS_ConstIterator
	FawnDS_ConstIterator::operator--(int)
   	{
	   	FawnDS_ConstIterator tmp(*this);
	   	operator--();
	   	return tmp;
   	}

	FawnDS_ConstIterator&
	FawnDS_ConstIterator::operator++()
   	{
	   	assert(elem);
	   	elem->Next();
	   	return *this;
   	}

	FawnDS_ConstIterator
	FawnDS_ConstIterator::operator++(int)
   	{
	   	FawnDS_ConstIterator tmp(*this);
	   	operator++();
	   	return tmp;
   	}

	const FawnDS_IteratorElem&
    FawnDS_ConstIterator::operator*() const
   	{
	   	assert(elem);
	   	return *elem;
   	}

	const FawnDS_IteratorElem*
    FawnDS_ConstIterator::operator->() const
   	{
	   	assert(elem);
	   	return elem;
   	}

	FawnDS_Iterator::FawnDS_Iterator()
		: FawnDS_ConstIterator()
	{
	}

    FawnDS_Iterator::FawnDS_Iterator(FawnDS_IteratorElem* elem)
		: FawnDS_ConstIterator(elem)
   	{
   	}

	FawnDS_Iterator::FawnDS_Iterator(const FawnDS_Iterator& it)
		: FawnDS_ConstIterator()
   	{
	   	*this = it;
   	}

	FawnDS_Iterator&
	FawnDS_Iterator::operator=(const FawnDS_Iterator& rhs)
   	{
	   	return (*this = static_cast<const FawnDS_ConstIterator&>(rhs));
   	}

	FawnDS_Iterator&
	FawnDS_Iterator::operator=(const FawnDS_ConstIterator& rhs)
   	{
        // same as FawnDS_ConstIterator::operator=()
	   	delete elem;
	   	if (rhs.elem)
		   	elem = rhs.elem->Clone();
	   	else
		   	elem = NULL;
	   	return *this;
   	}

	FawnDS_Iterator&
	FawnDS_Iterator::operator--()
   	{
	   	assert(elem);
	   	elem->Prev();
	   	return *this;
   	}

	FawnDS_Iterator
	FawnDS_Iterator::operator--(int)
   	{
	   	FawnDS_Iterator tmp(*this);
	   	operator--();
	   	return tmp;
   	}

	FawnDS_Iterator&
	FawnDS_Iterator::operator++()
   	{
	   	assert(elem);
	   	elem->Next();
	   	return *this;
   	}

	FawnDS_Iterator
	FawnDS_Iterator::operator++(int)
   	{
	   	FawnDS_Iterator tmp(*this);
	   	operator++();
	   	return tmp;
   	}

	FawnDS_IteratorElem&
    FawnDS_Iterator::operator*()
   	{
	   	assert(elem);
	   	return *elem;
   	}

	FawnDS_IteratorElem*
    FawnDS_Iterator::operator->()
   	{
	   	assert(elem);
	   	return elem;
   	}

} // namespace fawn
