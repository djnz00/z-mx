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

#include "src/models/wrappers/TableDockWidgetModelWrapper.h"
#include "src/subscribers/TableSubscriber.h"
#include "src/widgets/BasicTableWidget.h"
#include "QLinkedList"
#include "QDebug"
#include "src/factories/TableWidgetFactory.h"
#include "QTableWidget"
#include "src/distributors/DataDistributor.h"
#include "src/utilities/typeWrappers/MxTelemetryGeneralWrapper.h"


TableDockWidgetModelWrapper::TableDockWidgetModelWrapper(DataDistributor& a_dataDistributor) :
    DockWidgetModelWrapper(a_dataDistributor),
    m_tableSubscriberDB(new QList<QMap<QString, QPair<BasicTableWidget*, TableSubscriber*>>*>)
{
    qDebug() << "TableDockWidgetModelWrapper::TableDockWidgetModelWrapper()";

    // init default value
    m_defaultValue.first = nullptr;
    m_defaultValue.second = nullptr;

    //must be compatible with the signal-slot type,
    //to enable usage in run time
    // see https://doc.qt.io/qt-5/custom-types.html
    qRegisterMetaType<QLinkedList<QString>>();

    //init the QMap inside QList in m_tableList
    for (int var = 0; var < MxTelemetryGeneralWrapper::mxTypeSize(); ++var)
    {
        m_tableSubscriberDB->append(new QMap<QString, QPair<BasicTableWidget*, TableSubscriber*>>());
    }
}


TableDockWidgetModelWrapper::~TableDockWidgetModelWrapper()
{
    qDebug() << "            ~TableDockWidgetModelWrapper() - Begin";

    // # # # clean m_tempTable # # #
    // iterate over QList
    for (auto i = m_tableSubscriberDB->begin(); i != m_tableSubscriberDB->end(); i++)
    {
        //iterate over QMap
        for (auto k = (*i)->begin(); k !=  (*i)->end(); k++)
        {
            delete (k).value().second; //delete TableSubscriber*
            (k).value().second = nullptr;

            // ! very important ! before deleting set differnt parent,
            // to ensure that the dock widget which cotains the table
            // will not also attempt to delete the table
            (k).value().first->setParent(nullptr);
            delete (k).value().first;   //delete Basic Table Widger*
            (k).value().first = nullptr;
        }
        delete *i; //delete QMap* at this position
    }

    delete m_tableSubscriberDB;
    qDebug() << "            ~TableDockWidgetModelWrapper() - End";
}


QTableWidget* TableDockWidgetModelWrapper::getTable(const QString& a_mxTelemetryTypeName,
                                         const QString& a_mxTelemetryInstanceName)
{
    //translate a_mxTelemetryTypeName to corresponding number
    const int mxTelemetryTypeNameNumber = MxTelemetryGeneralWrapper::fromMxTypeNameToValue(a_mxTelemetryTypeName);

    // get the corresponding pair
    auto l_pair = getSubscriberPair(mxTelemetryTypeNameNumber, a_mxTelemetryInstanceName);

    // sanity check
    if ( (l_pair.first == nullptr && l_pair.second != nullptr)
            ||
         (l_pair.first != nullptr && l_pair.second == nullptr) )
    {
        qCritical() << "getTableSubscriberPair returned invalid pair for:"
                    << a_mxTelemetryTypeName
                    << a_mxTelemetryInstanceName;
        return nullptr;
    }

    // everything already exists, just need to subscribe again
    if (l_pair.first != nullptr && l_pair.second != nullptr)
    {
        qDebug() << "getTable already exists!";

        //subscribe
        m_dataDistributor.subscribe(mxTelemetryTypeNameNumber, l_pair.second);

        return l_pair.first;
    }


    qDebug() << "getTable create table and subscriber for the first time!";
    // create table
    l_pair.first = TableWidgetFactory::getInstance().getTableWidget(mxTelemetryTypeNameNumber, a_mxTelemetryInstanceName);

    //create subscriber
//    l_pair.second = TableSubscriberFactory::getInstance().getTableSubscriber(mxTelemetryTypeNameNumber);
    l_pair.second = new TableSubscriber(MxTelemetryGeneralWrapper::fromMxTypeNameToValue(a_mxTelemetryTypeName),
                                        a_mxTelemetryInstanceName);
//    qDebug() << "setTableName:" << a_mxTelemetryInstanceName;
    //l_pair.second->setAssociatedObjesctName(a_mxTelemetryInstanceName); //todo -> move into constrctor

    //add to the table
    m_tableSubscriberDB->at(mxTelemetryTypeNameNumber)->insert(a_mxTelemetryInstanceName, l_pair);

    //connect signal and slot
    QObject::connect(l_pair.second, &TableSubscriber::updateDone, l_pair.first, &BasicTableWidget::updateData);

    // subscribe
    m_dataDistributor.subscribe(mxTelemetryTypeNameNumber, l_pair.second);
    //return the table
    return l_pair.first;
}


void TableDockWidgetModelWrapper::unsubscribe(const int a_mxTelemetryType,
                                              const QString& a_mxTelemetryInstanceName) noexcept
{
    auto l_tableSubscriberInstance = getSubscriberPair(a_mxTelemetryType, a_mxTelemetryInstanceName).second;
    m_dataDistributor.unsubscribe(a_mxTelemetryType, l_tableSubscriberInstance);
}


QPair<BasicTableWidget*, TableSubscriber*> TableDockWidgetModelWrapper::getSubscriberPair(const int a_mxTelemetryTypeName,
                                                                                 const QString& a_mxTelemetryInstanceName) noexcept
{
    //get the map from the list
    auto l_map = m_tableSubscriberDB->value(a_mxTelemetryTypeName, nullptr);

    //sanity check
    if (l_map == nullptr)
    {
        qCritical() << "getTableSubscriberPair called with invalid (a_mxTelemetryTypeName, a_mxTelemetryInstanceName) ("
                    << a_mxTelemetryTypeName << a_mxTelemetryInstanceName << ") returning default value";
        return m_defaultValue;
    }

    // return the pair if exists or m_defaultValue
    return l_map->value(a_mxTelemetryInstanceName, m_defaultValue);
}







