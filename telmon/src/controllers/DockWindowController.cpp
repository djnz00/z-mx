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



#include "src/controllers/DockWindowController.h"
#include "QDockWidget"
#include "QDebug"

DockWindowController::DockWindowController(DataDistributor& a_dataDistributor,
                                           const QString& a_className,
                                           QObject* a_parent):
    BasicController(a_dataDistributor, a_className, a_parent)
{

}


DockWindowController::~DockWindowController()
{

}


bool DockWindowController::isDockWidgetExists(const QList<QDockWidget *>& a_currentDockList,
                                              const QString& a_objectName,
                                              QDockWidget*& a_dock) const noexcept
{
    bool l_contains = false;
    foreach (a_dock, a_currentDockList)
    {
        if (a_dock->windowTitle() == a_objectName)
        {
            // yes already exists
            l_contains = true;
            break;
        }
    }
    return l_contains;
}


void DockWindowController::initSubController(const int mxTelemetryType,
                               const QString& mxTelemetryInstanceName) noexcept
{
    qCritical() << __PRETTY_FUNCTION__
                << "of base class"
                << "mxTelemetryType:"
                << mxTelemetryType
                << "mxTelemetryInstanceName:"
                << mxTelemetryInstanceName;
}




