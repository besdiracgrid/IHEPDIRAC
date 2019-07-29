Ext.define('IHEPDIRAC.TaskManager.classes.TaskManager', {

    extend : 'Ext.dirac.core.Module',

    requires : ['Ext.util.*', 'Ext.panel.Panel', "Ext.form.field.Text", "Ext.button.Button", "Ext.menu.CheckItem", "Ext.menu.Menu", "Ext.form.field.ComboBox", "Ext.layout.*", "Ext.toolbar.Paging", "Ext.grid.Panel", "Ext.form.field.Date", "Ext.form.field.TextArea",
        "Ext.dirac.utils.DiracToolButton", "Ext.dirac.utils.DiracGridPanel", 'Ext.dirac.utils.DiracIdListButton', 'Ext.dirac.utils.DiracPageSizeCombo', "Ext.dirac.utils.DiracPagingToolbar", "Ext.dirac.utils.DiracApplicationContextMenu", "Ext.dirac.utils.DiracBaseSelector",
        "Ext.dirac.utils.DiracAjaxProxy", "Ext.data.ArrayStore", "Ext.dirac.utils.DiracJsonStore", "Ext.dirac.utils.DiracArrayStore"],

    loadState : function(data) {

      var me = this;

      me.grid.loadState(data);

      data.leftMenu.selectors['status'].data_selected = ['Init', 'Ready', 'Processing', 'Finished'];
      me.leftPanel.loadState(data);
    },

    dataFields : [{
          name : 'TaskIDcheckBox',
          mapping : 'TaskID'
        }, {
          name : 'TaskID',
          type : 'int'
        }, {
          name : 'TaskName',
        }, {
          name : 'StatusIcon',
          mapping : 'IconStatus'
        }, {
          name : 'IconStatus',
        }, {
          name : 'Status'
        }, {
          name : 'Owner'
        }, {
          name : 'OwnerDN'
        }, {
          name : 'OwnerGroup'
        }, {
          name : 'CreationTime',
          type : 'date',
          dateFormat : 'Y-m-d H:i:s'
        }, {
          name : 'UpdateTime',
          type : 'date',
          dateFormat : 'Y-m-d H:i:s'
        }, {
          name : 'Total'
        }, {
          name : 'Progress'
        }, {
          name : 'JobGroup'
        }, {
          name : 'Site'
        }],

    initComponent : function() {

        var me = this;

        me.launcher.title = "Task Manager";
        me.launcher.maximized = true;

        me.launcher.width  = 500
        me.launcher.height = 500
        me.launcher.y = 0

        Ext.apply(me, {
            layout : 'border',
            bodyBorder : false,
            defaults : {
                collapsible : true,
                split : true
            }
        });

        me.callParent(arguments);

    },

    buildUI : function() {

        var me = this;

        /*
         * -----------------------------------------------------------------------------------------------------------
         * DEFINITION OF THE LEFT PANEL
         * -----------------------------------------------------------------------------------------------------------
         */

        var selectors = {
          status : "Status",
          owner : "Owner",
          ownerGroup : "OwnerGroup",
        };

        var textFields = {
          'TaskID' : {
            name : "TaskID(s)",
            type : "number"
          }
        };

        var properties = [["NormalUser", "JobSharing", "owner"]];
        var map = [["status", "status"], ["owner", "owner"], ["ownerGroup", "ownerGroup"]];

        me.leftPanel = Ext.create('Ext.dirac.utils.DiracBaseSelector', {
            scope : me,
            cmbSelectors : selectors,
            textFields : textFields,
            datamap : map,
            url : me.applicationName + '/getSelectionData',
            properties : properties
        });

//        // all of the following does not functional
//
//        me.leftPanel.cmbSelectors['status'].afterRender = function() {
//            // not functional
//            me.leftPanel.cmbSelectors['status'].setValue(['Init', 'Ready', 'Processing', 'Finished']);
//        };
//
//        me.leftPanel.cmbSelectors['status'].addListener('load', function() {
//            me.leftPanel.cmbSelectors['status'].setValue(['Init', 'Ready', 'Processing', 'Finished']);
//          },
//          me
//        );
//
//        var oHeartbeat = new Ext.util.TaskRunner();
//
//        var oTask = {
//          run : function() {
//            if (!me.leftPanel.cmbSelectors['status'].isHidden()) {
//              console.log(me.leftPanel.cmbSelectors['status'].isVisible());
//              console.log(me.leftPanel.cmbSelectors['status']);
//              console.log(me.leftPanel.cmbSelectors['status'].getValue());
//              console.log(me.leftPanel.cmbSelectors['status'].setValue(['Init', 'Ready', 'Processing', 'Finished']));
//              console.log(me.leftPanel.cmbSelectors['status'].getValue());
//              oHeartbeat.stopAll()
//            }
//          },
//          interval : 0
//        };
//
//        oHeartbeat.start(Ext.apply(oTask, {interval : 1000}));


        /*
         * -----------------------------------------------------------------------------------------------------------
         * DEFINITION OF THE GRID
         * -----------------------------------------------------------------------------------------------------------
         */

        // data
        var oProxy = Ext.create('Ext.dirac.utils.DiracAjaxProxy', {
              url : GLOBAL.BASE_URL + me.applicationName + '/getTaskData'
            });

        me.dataStore = Ext.create("Ext.dirac.utils.DiracJsonStore", {
              autoLoad : false,
              proxy : oProxy,
              fields : me.dataFields,
              scope : me,
              remoteSort : false,
              autoLoad : true
            });

        // columns
        var oColumns = {
          "checkBox" : {
            "dataIndex" : "TaskIDcheckBox"
          },
          "TaskID" : {
            "dataIndex" : "TaskID",
            "properties" : {
              width : 60
            }
          },
          "TaskName" : {
            "dataIndex" : "TaskName",
            "editor": {
                allowBlank: false
            }
          },
          "None" : {
            "dataIndex" : "StatusIcon",
            "properties" : {
              width : 26,
              sortable : false,
              hideable : false,
              fixed : true,
              menuDisabled : true
            },
            "renderFunction" : "rendererStatus"
          },
          "Status" : {
            "dataIndex" : "Status"
          },
          "Jobs" : {
            "dataIndex" : "Total",
            "properties" : {
              width : 80
            },
          },
          "Progress (D|F|R|W|O)" : {
            "dataIndex" : "Progress",
            "properties" : {
              width : 150
            },
            "renderer" : me.__renderProgress
          },
          "CreationTime[UTC]" : {
            "dataIndex" : "CreationTime",
            "renderer" : Ext.util.Format.dateRenderer('Y-m-d H:i:s'),
            "properties" : {
              width : 150
            }
          },
          "UpdateTime[UTC]" : {
            "dataIndex" : "UpdateTime",
            "renderer" : Ext.util.Format.dateRenderer('Y-m-d H:i:s'),
            "properties" : {
              width : 150
            }
          },
          "Site" : {
            "dataIndex" : "Site",
            "properties" : {
              width : 180
            }
          },
          "JobGroup" : {
            "dataIndex" : "JobGroup",
            "properties" : {
              width : 150
            }
          },
          "OwnerDN" : {
            "dataIndex" : "OwnerDN",
            "properties" : {
              hidden : true
            }
          },
          "Owner" : {
            "dataIndex" : "Owner"
          },
          "OwnerGroup" : {
            "dataIndex" : "OwnerGroup",
          },
        };

        // menu
        var menuitems = {
          'Visible' : [{
                "text" : "Progress",
                "handler" : me.__getTaskProgress,
                "properties" : {
                  tooltip : 'Click to show task progress'
                }
              }, {
                "text" : "Jobs Statistics",
                "handler" : me.__getTaskJobsStatistics,
                "properties" : {
                  tooltip : 'Click to show task jobs statistics'
                }
              }, {
                "text" : "-"
              },// separator
              {
                "text" : "Information",
                "handler" : me.__getTaskInfo,
                "properties" : {
                  tooltip : 'Click to show task information'
                }
              }, {
                "text" : "History",
                "handler" : me.__getTaskHistory,
                "properties" : {
                  tooltip : 'Click to show task history'
                }
              }, {
                "text" : "-"
              },// separator
              {
                "text" : "Show Jobs",
                "handler" : me.__getTaskJobs,
                "properties" : {
                  tooltip : 'Click to show task jobs in job monitor page'
                }
              }, {
                "text" : "Jobs Information",
                "handler" : me.__getJobsInfo,
                "properties" : {
                  tooltip : 'Click to show task jobs information'
                }
              }, {
                "text" : "-"
              },// separator
              {
                "text" : "Rename",
                "handler" : me.__renameTask,
                "properties" : {
                  tooltip : 'Click to rename the task'
                }
              }, {
                "text" : "-"
              },// separator
              {
                "text" : "Reschedule Failed Jobs",
                "handler" : me.__rescheduleTask,
                "arguments" : [["Failed"], true],
                "properties" : {
                  tooltip : 'Click to reschedule failed jobs in the task',
                  iconCls : "dirac-icon-reschedule"
                }
              }, {
                "text" : "Reschedule All Jobs",
                "handler" : me.__rescheduleTask,
                "arguments" : [[], true],
                "properties" : {
                  tooltip : 'Click to reschedule all jobs in the task',
                  iconCls : "dirac-icon-reschedule"
                }
              }, {
                "text" : "Delete",
                "handler" : me.__deleteTask,
                "arguments" : [true],
                "properties" : {
                  tooltip : 'Click to delete the task and all jobs',
                  iconCls : "dirac-icon-delete"
                }
              }]
        };

        me.contextGridMenu = new Ext.dirac.utils.DiracApplicationContextMenu({
              menu : menuitems,
              scope : me
            });

        // toolbar
        var toolButtons = {
          'Visible' : [{
                "text" : "",
                "handler" : me.__rescheduleTask,
                "arguments" : [["Failed"], ""],
                "properties" : {
                  tooltip : "Reschedule Failed Jobs in the Task",
                  iconCls : "dirac-icon-reschedule"
                }
              }, {
                "text" : "",
                "handler" : me.__deleteTask,
                "arguments" : [""],
                "properties" : {
                  tooltip : "Delete the Task and all jobs",
                  iconCls : "dirac-icon-delete"
                }
              }]
        };

        me.pagingToolbar = Ext.create("Ext.dirac.utils.DiracPagingToolbar", {
              toolButtons : toolButtons,
              //property : "JobAdministrator",
              store : me.dataStore,
              scope : me
            });

        // create grid
        me.grid = Ext.create('Ext.dirac.utils.DiracGridPanel', {
              store : me.dataStore,
              // features: [{ftype:'grouping'}],
              oColumns : oColumns,
              contextMenu : me.contextGridMenu,
              pagingToolbar : me.pagingToolbar,
              scope : me
            });

        me.grid.columns[1].setSortState("DESC");


        me.leftPanel.setGrid(me.grid);

        /*
         * -----------------------------------------------------------------------------------------------------------
         * The page
         * -----------------------------------------------------------------------------------------------------------
         */

        me.add([me.leftPanel, me.grid]);

    },

    __renderProgress : function(value) {

      var jobStatuses = ['Done', 'Failed', 'Running', 'Waiting', 'Deleted'];
      var statusColor = ['green', 'red', 'blue', 'orange', 'black'];
      var statusNumber = [];

      for (var i in jobStatuses) {
        jobStatus = jobStatuses[i]
        if (value[jobStatus] == undefined)
          value[jobStatus] = 0;
        statusNumber.push(Ext.String.format('<span style="color:{0}">{1}</span>', statusColor[i], value[jobStatus]));
      }

      return Ext.String.format('{0} | {1} | {2} | {3} | {4}', statusNumber[0], statusNumber[1], statusNumber[2], statusNumber[3], statusNumber[4]);

    },

    __renameTask : function() {

        var me = this;
        var oId = GLOBAL.APP.CF.getFieldValueFromSelectedRow(me.grid, "TaskID");

        Ext.MessageBox.prompt('Task ' + oId, 'Please enter the new task name:',
              function(btn, text) {
                if (btn != 'ok')
                  return;

                Ext.Ajax.request({
                      url : GLOBAL.BASE_URL + me.applicationName + '/renameTask',
                      method : 'POST',
                      params : {
                        TaskID : oId,
                        NewName : text
                      },
                      success : function(response) {
                        var jsonData = Ext.JSON.decode(response.responseText);
                        if (jsonData['success'] == 'false') {
                          GLOBAL.APP.CF.alert('Error: ' + jsonData['error'], "error");
                          return;
                        }

                        me.leftPanel.oprLoadGridData();
                      }
                  });
              });

    },

    __getTaskProgress : function() {

        var me = this;
        var oId = GLOBAL.APP.CF.getFieldValueFromSelectedRow(me.grid, "TaskID");

        me.getContainer().body.mask("Wait ...");

        Ext.Ajax.request({
              url : GLOBAL.BASE_URL + me.applicationName + '/getTaskProgress',
              method : 'POST',
              params : {
                TaskID : oId
              },
              success : function(response) {

                me.getContainer().body.unmask();
                var jsonData = Ext.JSON.decode(response.responseText);
                if (jsonData['success'] == 'false') {
                  GLOBAL.APP.CF.alert('Error: ' + jsonData['error'], "error");
                  return;
                }

                me.getContainer().oprPrepareAndShowWindowGrid(jsonData['result'], "Progress for task " + oId, ["status", "jobnum"], [{
                          text : 'Job Status',
                          flex : 1,
                          sortable : true,
                          dataIndex : 'status'
                        }, {
                          text : 'Job Number',
                          flex : 1,
                          sortable : true,
                          dataIndex : 'jobnum'
                        }]);

                me.leftPanel.oprLoadGridData();
              },
              failure : function(response) {
                me.getContainer().body.unmask();
              }
          });

    },

    __getTaskInfo : function() {

        var me = this;
        var oId = GLOBAL.APP.CF.getFieldValueFromSelectedRow(me.grid, "TaskID");

        me.getContainer().body.mask("Wait ...");

        Ext.Ajax.request({
              url : GLOBAL.BASE_URL + me.applicationName + '/getTaskInfo',
              method : 'POST',
              params : {
                TaskID : oId
              },
              success : function(response) {

                me.getContainer().body.unmask();
                var jsonData = Ext.JSON.decode(response.responseText);
                if (jsonData['success'] == 'false') {
                  GLOBAL.APP.CF.alert('Error: ' + jsonData['error'], "error");
                  return;
                }

                me.getContainer().oprPrepareAndShowWindowGrid(jsonData['result'], "Information for task " + oId, ["name", "value"], [{
                          text : 'Name',
                          flex : 1,
                          sortable : true,
                          dataIndex : 'name'
                        }, {
                          text : 'Value',
                          flex : 1,
                          sortable : true,
                          dataIndex : 'value'
                        }]);
              },
              failure : function(response) {
                me.getContainer().body.unmask();
              }
          });

    },

    __getTaskHistory : function() {

        var me = this;
        var oId = GLOBAL.APP.CF.getFieldValueFromSelectedRow(me.grid, "TaskID");

        me.getContainer().body.mask("Wait ...");

        Ext.Ajax.request({
              url : GLOBAL.BASE_URL + me.applicationName + '/getTaskHistory',
              method : 'POST',
              params : {
                TaskID : oId
              },
              success : function(response) {

                me.getContainer().body.unmask();
                var jsonData = Ext.JSON.decode(response.responseText);
                if (jsonData['success'] == 'false') {
                  GLOBAL.APP.CF.alert('Error: ' + jsonData['error'], "error");
                  return;
                }

                me.getContainer().oprPrepareAndShowWindowGrid(jsonData['result'], "History for task " + oId, ["status", "time", "description"], [{
                          text : 'Status',
                          flex : 1,
                          sortable : true,
                          dataIndex : 'status'
                        }, {
                          text : 'Time',
                          flex : 1,
                          sortable : true,
                          dataIndex : 'time'
                        }, {
                          text : 'Description',
                          flex : 1,
                          sortable : true,
                          dataIndex : 'description'
                        }]);
              },
              failure : function(response) {
                me.getContainer().body.unmask();
              }

          });

    },

    __getTaskJobs : function() {

        var me = this;
        var oId = GLOBAL.APP.CF.getFieldValueFromSelectedRow(me.grid, "TaskID");

        Ext.Ajax.request({
              url : GLOBAL.BASE_URL + me.applicationName + '/getTaskJobs',
              method : 'POST',
              params : {
                TaskID : oId
              },
              success : function(response) {

                var jsonData = Ext.JSON.decode(response.responseText);
                if (jsonData['success'] == 'false') {
                  GLOBAL.APP.CF.alert('Error: ' + jsonData['error'], "error");
                  return;
                }

                var jobIDs = jsonData['result'];

                var oSetupData = {};
                if (GLOBAL.VIEW_ID == "desktop") { // we needs these
                  // information only
                  // for the desktop
                  // layout.

                  var oDimensions = GLOBAL.APP.MAIN_VIEW.getViewMainDimensions();
                  oSetupData.x = 0;
                  oSetupData.y = 0;
                  oSetupData.width = oDimensions[0];
                  oSetupData.height = oDimensions[1];
                  oSetupData.currentState = "";

                  oSetupData.desktopStickMode = 0;
                  oSetupData.hiddenHeader = 1;
                  oSetupData.i_x = 0;
                  oSetupData.i_y = 0;
                  oSetupData.ic_x = 0;
                  oSetupData.ic_y = 0;

                }

                oSetupData.data = {

                  leftMenu : {
                    JobID : jobIDs.join(',')
                  }
                };

                GLOBAL.APP.MAIN_VIEW.createNewModuleContainer({
                      objectType : "app",
                      moduleName : 'DIRAC.JobMonitor.classes.JobMonitor',
                      setupData : oSetupData
                    });
              }

          });
    },

    __getTaskJobsStatistics : function() {

        var me = this;
        var oId = GLOBAL.APP.CF.getFieldValueFromSelectedRow(me.grid, "TaskID");

        me.getContainer().body.mask("Wait ...");

        Ext.Ajax.request({
              url : GLOBAL.BASE_URL + me.applicationName + '/getTaskJobsStatistics',
              method : 'POST',
              params : {
                TaskID : oId
              },
              success : function(response) {

                me.getContainer().body.unmask();
                var jsonData = Ext.JSON.decode(response.responseText);
                if (jsonData['success'] == 'false') {
                  GLOBAL.APP.CF.alert('Error: ' + jsonData['error'], "error");
                  return;
                }

                me.getContainer().oprPrepareAndShowWindowGrid(jsonData['result'], "Statistics for task " + oId, ["status", "jobnum"], [{
                          text : 'Status Type',
                          flex : 1,
                          sortable : true,
                          dataIndex : 'status'
                        }, {
                          text : 'Job Number',
                          flex : 1,
                          sortable : true,
                          dataIndex : 'jobnum'
                        }]);
              },
              failure : function(response) {
                me.getContainer().body.unmask();
              }
          });

    },

    __getJobsInfo : function() {

        var me = this;
        var oId = GLOBAL.APP.CF.getFieldValueFromSelectedRow(me.grid, "TaskID");

        me.getContainer().body.mask("Wait ...");

        Ext.Ajax.request({
              url : GLOBAL.BASE_URL + me.applicationName + '/getTaskJobList',
              method : 'POST',
              params : {
                TaskID : oId
              },
              success : function(response) {

                me.getContainer().body.unmask();
                var jsonData = Ext.JSON.decode(response.responseText);
                if (jsonData['success'] == 'false') {
                  GLOBAL.APP.CF.alert('Error: ' + jsonData['error'], "error");
                  return;
                }

                var oWindow = me.getContainer().createChildWindow('Jobs Information (click to show details)', false, 500, 400);

                var oFields = ['jobID', 'status'];

                var oData = jsonData['result'];

                var oStore = new Ext.data.ArrayStore({
                      fields : oFields,
                      data : oData
                    });

                var oColumns = [{
                          text : 'JobID',
                          flex : 1,
                          sortable : true,
                          dataIndex : 'jobID'
                        }, {
                          text : 'Status',
                          flex : 1,
                          sortable : true,
                          dataIndex : 'status'
                        }];

                var oGrid = Ext.create('Ext.grid.Panel', {
                      store : oStore,
                      region : 'west',
                      columns : oColumns,
                      width : '200',
                      height : '200',
                      viewConfig : {
                        stripeRows : true,
                        enableTextSelection : true
                      },
                      listeners : {

                        select : function(oTable, record, index, eOpts) {
                          var oId = record.get("jobID");

                          me.getContainer().body.mask("Wait ...");

                          Ext.Ajax.request({
                                url : GLOBAL.BASE_URL + me.applicationName + '/getTaskJobInfo',
                                method : 'POST',
                                params : {
                                  JobID : oId
                                },
                                success : function(response) {

                                  me.getContainer().body.unmask();
                                  var jsonData = Ext.JSON.decode(response.responseText);
                                  if (jsonData['success'] == 'false') {
                                    GLOBAL.APP.CF.alert('Error: ' + jsonData['error'], "error");
                                    return;
                                  }

                                  me.getContainer().oprPrepareAndShowWindowGrid(jsonData['result'], "Job information " + oId, ["name", "value"], [{
                                            text : 'Name',
                                            flex : 1,
                                            sortable : true,
                                            dataIndex : 'name'
                                          }, {
                                            text : 'Value',
                                            flex : 1,
                                            sortable : true,
                                            dataIndex : 'value'
                                          }]);
                                },
                                failure : function(response) {
                                  me.getContainer().body.unmask();
                                }
                            });
                          return false;
                        }

                      }
                    });

                var oInfoGrid = Ext.create('Ext.grid.Panel', {
//                      store : oStore,
                      region : 'east',
                      columns : oColumns,
                      viewConfig : {
                        stripeRows : true,
                        enableTextSelection : true
                      },
                      listeners : {
                      }
                    });

//                oWindow.add([oGrid, oInfoGrid]);
                oWindow.add(oGrid);
                oWindow.show();

              },
              failure : function(response) {
                me.getContainer().body.unmask();
              }
          });

    },


    __rescheduleTask : function(jobStatus, useSelectedTaskId) {

        var me = this;
        var oItems = [];
        var oId = null;
        if (useSelectedTaskId) {
          var oId = GLOBAL.APP.CF.getFieldValueFromSelectedRow(me.grid, "TaskID");
        }

        if ((oId == null) || (oId == '') || (oId == undefined)) {

          var oElems = Ext.query("#" + me.id + " input.checkrow");

          for (var i = 0; i < oElems.length; i++)
            if (oElems[i].checked)
              oItems.push(oElems[i].value);

          if (oItems.length < 1) {
            GLOBAL.APP.CF.alert('No tasks were selected', "error");
            return;
          }

        } else {
          oItems[0] = oId;
        }

        Ext.MessageBox.confirm('Confirm', 'Are you sure you want to reschedule all ' + jobStatus + ' jobs in task ' + oItems + '?',
              function(btn) {
                if (btn != 'yes')
                  return;

                me.getContainer().body.mask("Wait ...");

                Ext.Ajax.request({
                      url : GLOBAL.BASE_URL + me.applicationName + '/rescheduleTask',
                      method : 'POST',
                      params : {
                        TaskID : oItems.join(","),
                        JobStatus : jobStatus.join(",")
                      },
                      success : function(response) {

                        me.getContainer().body.unmask();
                        var jsonData = Ext.JSON.decode(response.responseText);
                        if (jsonData['success'] == 'false') {
                          GLOBAL.APP.CF.alert('Error: ' + jsonData['error'], "error");
                          return;
                        }

                        me.leftPanel.oprLoadGridData();
                      },
                      failure : function(response) {
                        me.getContainer().body.unmask();
                      }
                  });
              });

    },

    __deleteTask : function(useSelectedTaskId) {

        var me = this;
        var oItems = [];
        var oId = null;
        if (useSelectedTaskId) {
          var oId = GLOBAL.APP.CF.getFieldValueFromSelectedRow(me.grid, "TaskID");
        }

        if ((oId == null) || (oId == '') || (oId == undefined)) {

          var oElems = Ext.query("#" + me.id + " input.checkrow");

          for (var i = 0; i < oElems.length; i++)
            if (oElems[i].checked)
              oItems.push(oElems[i].value);

          if (oItems.length < 1) {
            GLOBAL.APP.CF.alert('No tasks were selected', "error");
            return;
          }

        } else {
          oItems[0] = oId;
        }

        Ext.MessageBox.confirm('Confirm', 'Are you sure you want to delete task ' + oItems + ' and all the jobs?',
              function(btn) {
                if (btn != 'yes')
                  return;

                me.getContainer().body.mask("Wait ...");

                Ext.Ajax.request({
                      url : GLOBAL.BASE_URL + me.applicationName + '/deleteTask',
                      method : 'POST',
                      params : {
                        TaskID : oItems.join(",")
                      },
                      success : function(response) {

                        me.getContainer().body.unmask();
                        var jsonData = Ext.JSON.decode(response.responseText);
                        if (jsonData['success'] == 'false') {
                          GLOBAL.APP.CF.alert('Error: ' + jsonData['error'], "error");
                          return;
                        }

                        me.leftPanel.oprLoadGridData();
                      },
                      failure : function(response) {
                        me.getContainer().body.unmask();
                      }
                  });
              });

    },

});
