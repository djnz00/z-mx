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

// based on:
// https://stackoverflow.com/questions/86582/singleton-how-should-it-be-used



#ifndef TABLEWIDGETFACTORY_H
#define TABLEWIDGETFACTORY_H

class BasicTableWidget;
class QString;

class TableWidgetFactory
{
private:
    // Private Constructor
    TableWidgetFactory();
    // Stop the compiler generating methods of copy the object
    TableWidgetFactory(TableWidgetFactory const& copy);            // Not Implemented
    TableWidgetFactory& operator=(TableWidgetFactory const& copy); // Not Implemented

public:
    static TableWidgetFactory& getInstance()
    {
        // The only instance
        // Guaranteed to be lazy initialized
        // Guaranteed that it will be destroyed correctly
        static TableWidgetFactory m_instance;
        return m_instance;
    }

    // Not responsible for deallocating
    BasicTableWidget* getTableWidget(const int a_tableType,
                                     const QString& a_mxTelemetryInstanceName) const noexcept;
};

#endif // TABLEWIDGETFACTORY_H
