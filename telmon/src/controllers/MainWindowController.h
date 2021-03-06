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


#ifndef MAINWINDOWCONTROLLER_H
#define MAINWINDOWCONTROLLER_H

#include <QMainWindow>

class MainWindowView;
class MainWindowModel;
class BasicController;
class StatusBarWidget;


class MainWindowController : public QMainWindow
{
    Q_OBJECT
public:
    explicit MainWindowController(QWidget *parent = nullptr);
    ~MainWindowController() override;


    void dockWindowsManagerTreeWidgetContextMenuSequence(
                            const unsigned int a_dockWindowType,
                            const QString& a_mxTelemetryInstanceName,
                            const int a_mxTelemetryType) noexcept;


    void dockWindowsManagerFirstTimeDataInsertionToTreeSequence(
            const int a_mxTelemetryType,
            const QString& a_mxTelemetryInstanceName) noexcept;


    bool showPolicy(const unsigned int a_controllerType,
                    const int a_mxType,
                    const QString& a_mxInstance) const noexcept;

public slots:
    void closeDockWidget(const int) noexcept;
    StatusBarWidget* getStatusBar() noexcept;

private:
    MainWindowModel *m_mainWindowModel;
    MainWindowView *m_mainWindowView;
    QMap<unsigned int, BasicController*>* m_controllersDB;

    void initController(const unsigned int a_key) noexcept;
    void terminateController(const unsigned int a_key) noexcept;

    void createActions() noexcept;

    /**
     * @brief used to load .css file for appearnce
     */
    void setAppearance(const QString& a_pathToFile = QString()) noexcept;

    inline static const QString DEFAULT_STYLE_PATH = "src/styles/dark_default.qss";
    inline static const QString CURRENT_VER = "1.0";

};

#endif // MAINWINDOWCONTROLLER_H
