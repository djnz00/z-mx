//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2

/*
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */


#ifndef TABLESUBSCRIBER_H
#define TABLESUBSCRIBER_H

#include "OneMxTypeDataSubscriber.h"
#include "QObject"


class TableSubscriber :  public QObject, public OneMxTypeDataSubscriber
{
    Q_OBJECT

public:
    TableSubscriber(const int a_mxTelemetryType,
                    const QString& a_instance);
    virtual ~TableSubscriber() override;
    virtual void update(void* a_mxTelemetryMsg) override final;


signals:
    // must be compatible with qRegisterMetaType!
    void updateDone(QLinkedList<QString>);

};

#endif // TABLESUBSCRIBER_H


