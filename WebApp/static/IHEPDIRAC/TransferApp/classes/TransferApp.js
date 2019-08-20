Ext.define('IHEPDIRAC.TransferApp.classes.TransferApp', {
    extend : 'Ext.dirac.core.Module',
    requires : ['Ext.util.*', 
                'Ext.panel.Panel', 
                "Ext.form.field.Text", 
                "Ext.button.Button", 
                "Ext.menu.Menu", 
                "Ext.form.field.ComboBox", 
                "Ext.layout.*", 
                "Ext.form.field.Date", 
                "Ext.form.field.TextArea", 
                "Ext.form.field.Checkbox", 
                "Ext.form.FieldSet",
                "Ext.dirac.utils.DiracMultiSelect", 
                "Ext.toolbar.Toolbar", 
                "Ext.data.Record", 
                // 'Ext.grid.PagingScroller',
                'Ext.Array', 
                'Ext.data.TreeStore', 
                'Ext.layout.container.Accordion',
                "Ext.ux.form.MultiSelect"
               ],
    // = initComponent =
    initComponent: function() {
        var me = this;
        console.log(me);
    
        //setting the title of the application
        me.launcher.title = "Transfer Application";
        //setting the maximized state
        me.launcher.maximized = true;
    
        Ext.apply(me, {
              layout : 'border',
              bodyBorder : false,
              defaults : {
                collapsible : true,
                split : true
              },
              items : [],
              header : false
            });

        me.callParent(arguments);


    },

    // = buildUI =
    buildUI : function() {
        var me = this;
        console.log(me);

        // top toolbar -> me.top_toolbar
        me.build_toolbar();
        // main panel -> me.main_panel
        me.build_mainpanel();

        // + requests
        me.build_panel_requests();
        // + datasets
        me.build_panel_datasets();

        me.add([me.main_panel]);
        console.log("hello");
    }, 

    build_toolbar : function() {
        var me = this;
        console.log(me);
        me.top_toolbar = Ext.create('Ext.toolbar.Toolbar', {
              dock : 'top',
              border : false,
              layout : {
                pack : 'center'
              },
              items : [{
                   xtype: "button",
                   text: "Requests",
                   handler : function() {
                      me.main_panel.getLayout().setActiveItem(0);
                   },
                   toggleGroup : me.id + "-ids-submodule",
                   allowDepress : false
                }, {
                   xtype: "button",
                   text: "Datasets",
                   handler : function() {
                      me.main_panel.getLayout().setActiveItem(1);
                   },
                   toggleGroup : me.id + "-ids-submodule",
                   allowDepress : false
              }]
              });
        me.top_toolbar.items.getAt(0).toggle();
    },

    build_mainpanel : function() {
        var me = this;
        console.log(me);
        me.main_panel = new Ext.create('Ext.panel.Panel', {
              floatable : false,
              layout : 'card',
              region : "center",
              header : false,
              border : false,
              dockedItems : [me.top_toolbar]
            });

    }, 

    // == requests ==

    build_panel_requests : function() {
        var me = this;
        console.log(me);
        // + requests list
        me.build_panel_requests_list();
        // + file list in request
        me.build_panel_files_in_request();
        me.panel_requests = new Ext.create('Ext.panel.Panel', {
              id : "gPanelRequests",
              floatable : false,
              layout : 'accordion',
              // layout : 'column',
              header : false,
              border : false,
              items : [me.panel_requests_list, me.panel_files_in_request]
            });

        me.main_panel.add([me.panel_requests]);
    },

    // === requests: request list ===

    create_datastore_requests_list : function() {
        var me = this;
        console.log(me);
        me.datastore_request_list = new Ext.data.JsonStore({
            proxy : {
                type : 'ajax',
                method : 'POST',
                url : GLOBAL.BASE_URL + 'TransferApp/requestList',
                reader : {
                  type : 'json',
                  root : 'result'
                },
                timeout : 1800000
            },
            autoLoad : true,
            fields : [
                {
                    name: 'id',
                    type: 'int',
                },
                {
                    name: 'owner',
                    type: 'string',
                },
                {
                    name: 'dataset',
                    type: 'string',
                },
                {
                    name: 'srcSE',
                    type: 'string',
                },
                {
                    name: 'dstSE',
                    type: 'string',
                },
                {
                    name: 'protocol',
                    type: 'string',
                },
                {
                    name: 'submitTime',
                    type: 'string',
                },
                {
                    name: 'status',
                    type: 'string',
                },
            ],
            listeners: {
                load : function(oStore, records, successful, eOpts) {
                  var bResponseOK = (oStore.proxy.reader.rawData["success"] == "true");
                  if (!bResponseOK) {
                    GLOBAL.APP.CF.alert(oStore.proxy.reader.rawData["error"], "info");
                    if (parseInt(oStore.proxy.reader.rawData["total"], 10) == 0) {
                      me.dataStore.removeAll();
                    }
                  } else {
                    console.log(records);
                  }
                },

            },
        });
    },
    build_panel_requests_list : function() {
        var me = this;
        console.log(me);

        // -> me.datastore_request_list
        me.create_datastore_requests_list();
        var sm = Ext.create('Ext.selection.RowModel');
        me.panel_requests_list = new Ext.create('Ext.grid.Panel', {
            // autoScroll: true,
            // verticalScroller: {
            //     xtype: 'paginggridscroller',
            //     activePrefetch: false
            // },
            id: "gPanelRequestsList",
            store: me.datastore_request_list,
            columns: [
                {
                    text: "id",
                    dataIndex: "id",
                },
                {
                    text: "owner",
                    dataIndex: "owner",
                },
                {
                    text: "dataset",
                    dataIndex: "dataset",
                },
                {
                    text: "src SE",
                    dataIndex: "srcSE",
                },
                {
                    text: "dst SE",
                    dataIndex: "dstSE",
                },
                {
                    text: "protocol",
                    dataIndex: "protocol",
                },
                {
                    text: "submit time",
                    dataIndex: "submitTime",
                },
                {
                    text: "status",
                    dataIndex: "status",
                },
            ],

            title: "Requests list",
            dockedItems: [
                {
                xtype: "toolbar",
                items: [
                {
                    xtype: "button",
                    text: "new",
                    tooltip: "create new transfer request",
                    handler: me.build_panel_create_new_request,
                },
                {
                    // xtype: 'tbseparator',
                    xtype: 'tbspacer',
                },
                {
                    xtype: "button",
                    text: "view",
                    tooltip: "view files in current request",
                    // handler: me.view_files_in_current_request,
                    handler: function() {
                        console.log(me);
                        var selectedRows = me.panel_requests_list.getSelectionModel().getSelection();
                        console.log(selectedRows);
                        for (var i = 0; i < selectedRows.length; ++i) {
                            // console.log(selectedRows[i]);
                            // console.log(selectedRows[i].get("id"));
                            var reqid = selectedRows[i].get("id");
                            console.log(reqid);
                            // after get the datasetid, 
                            // we need to show them in files list.
                            me.datastore_files_in_request.load({
                                params: {
                                    reqid: reqid,
                                },
                                callback: function(records, operation, success) {
                                    // do something after the load finishes
                                    if (success) {
                                        // show the value
                                        me.panel_files_in_request.expand();
                                    }
                                },
                            });
                        }
                    }

                },
                {
                    // xtype: 'tbseparator',
                    xtype: 'tbspacer',
                },
                {
                    xtype: "button",
                    text: "refresh",
                    handler: me.refresh_requests_list,
                },
                ],
                }
            ],
        });
    },

    create_datastore_files_in_request : function() {
        var me = this;
        console.log(me);
        me.datastore_files_in_request = new Ext.data.JsonStore({
            proxy : {
                type : 'ajax',
                url : GLOBAL.BASE_URL + 'TransferApp/requestListFiles',
                reader : {
                  type : 'json',
                  root : 'result'
                },
                method : 'POST',
                timeout : 1800000
            },
            autoLoad : false,
            autoSync : true,
            fields : [
                {
                    name: 'id',
                    type: 'int',
                },
                {
                    name: 'LFN',
                    type: 'string',
                },
                {
                    name: 'starttime',
                    type: 'string',
                },
                {
                    name: 'finishtime',
                    type: 'string',
                },
                {
                    name: 'status',
                    type: 'string',
                },
                {
                    name: 'error',
                    type: 'string',
                },

            ],
            listeners: {
                load : function(oStore, records, successful, eOpts) {
                  var bResponseOK = (oStore.proxy.reader.rawData["success"] == "true");
                  if (!bResponseOK) {
                    GLOBAL.APP.CF.alert(oStore.proxy.reader.rawData["error"], "info");
                    if (parseInt(oStore.proxy.reader.rawData["total"], 10) == 0) {
                      me.dataStore.removeAll();
                    }
                  } else {
                    console.log(records);
                  }
                },

            },
        });
    },
    build_panel_files_in_request : function() {
        var me = this;
        console.log(me);
        // -> me.datastore_files_in_request
        me.create_datastore_files_in_request();
        var sm = Ext.create('Ext.selection.RowModel');
        me.panel_files_in_request = new Ext.create('Ext.grid.Panel', {
            id: "gPanelFilesInRequests",
            selModel : sm,
            // autoScroll: true,
            // verticalScroller: {
            //     xtype: 'paginggridscroller',
            //     activePrefetch: false
            // },
            store: me.datastore_files_in_request,
            columns: [
                {
                    text: "id",
                    dataIndex: "id",
                },
                {
                    text: "LFN",
                    dataIndex: "LFN",
                },
                {
                    text: "start",
                    dataIndex: "starttime",
                },
                {
                    text: "finish",
                    dataIndex: "finishtime",
                },
                {
                    text: "status",
                    dataIndex: "status",
                },
                {
                    text: "error",
                    dataIndex: "error",
                    renderer: function(value, metadata, record, rowIndex, colIndex, store) {
                        // console.log(record);
                        // console.log(record.data.error);
                        if (record.data && record.data.error && record.data.error.length>0) {
                            return "<a>Error</a>";
                        }
                        return "OK";
                    },
                },
            ],
            title: "Files list",
            dockedItems: [
                {
                xtype: "toolbar",
                items: [
                    {
                    xtype: "button",
                    text: "refresh",
                    handler: me.refresh_requests_file_list,
                    },
                    {
                    xtype: "button",
                    text: "show error",
                    handler: function() {
                        var selectedRows = me.panel_files_in_request.getSelectionModel().getSelection();
                        // console.log(selectedRows);
                        for (var i = 0; i < selectedRows.length; ++i) {
                            //console.log(selectedRows[i].get("error"));
                            plain_error = selectedRows[i].get("error");
                            Ext.create('Ext.window.Window', {
                                closable: true,
                                width: 600,
                                height: 400,
                                //autoHeight: true,
                                autoScroll: true,
                                title: "Error Info",
                                layout: "fit",
                                html: "<pre>"+plain_error+"</pre>"
                            }).show();
                        }

                    }
                    },
                ],
                },
            ],
        });
    },

    // === requests: new request ===
    build_panel_create_new_request: function() {
        // Ext.MessageBox.alert('Rendered One', 'Tab Two was rendered.');
        // Create a Form Panel
        var panel = Ext.create('Ext.form.Panel', {
            // title: 'Simple Form',
            bodyPadding: 5,
            width: 350,
        
            // The form will submit an AJAX request to this URL when submitted
            url: GLOBAL.BASE_URL + 'TransferApp/requestNew',
        
            // Fields will be arranged vertically, stretched to full width
            layout: 'anchor',
            defaults: {
                anchor: '100%'
            },
        
            // The fields
            defaultType: 'textfield',
            items: [{
                fieldLabel: 'Dataset Name',
                name: 'dataset',
                allowBlank: false
            },{
                fieldLabel: 'src SE',
                name: 'srcse',
                allowBlank: false
            },{
                fieldLabel: 'dst SE',
                name: 'dstse',
                allowBlank: false
            },{
                xtype: 'combo',
                fieldLabel: 'Protocol',
                name: 'protocol',
                store: ["DIRACDMS", "DIRACFTS"],
                forceSelect: true,
                queryMode: 'local',
                value: 'DIRACDMS',
                allowBlank: false
            }],
        
            // Reset and Submit buttons
            buttons: [{
                text: 'Reset',
                handler: function() {
                    this.up('form').getForm().reset();
                }
            }, {
                text: 'Submit',
                formBind: true, //only enabled once the form is valid
                disabled: true,
                handler: function() {
                    var form = this.up('form').getForm();
                    if (form.isValid()) {
                      try {
                        form.submit({
                            success: function(form, action) {
                                // reload
                                var panel_requests_list = Ext.getCmp("gPanelRequestsList");
                                panel_requests_list.getStore().reload();
                                // close
                                var win = Ext.getCmp("gNewTransferRequest");
                                win.close();
                            },
                            failure: function(form, action) {
                                console.log(form);
                                console.log(action);
                                if (action.response.status == 400) {
                                    Ext.Msg.alert("Failed", action.response.responseText);
                                }
                                // reload
                                var panel_requests_list = Ext.getCmp("gPanelRequestsList");
                                panel_requests_list.getStore().reload();
                            }
                        });
                      } catch(e) {
                        alert('Error: ' + e.name + ': ' + e.message);
                      }
                    }
                }
            }],
            renderTo: Ext.getBody()
        });
        // Create a window/Form
        Ext.create('Ext.window.Window', {
            id : "gNewTransferRequest",
            title: 'New Transfer Request',
            height: 200,
            width: 400,
            layout: 'fit',
            items: [panel],
        }).show(); 
    },
    // === requests: view request ===
    view_files_in_current_request: function() {
         Ext.MessageBox.alert('Rendered One', 'Tab Two was rendered.');
    },
    // === requests: refresh request ===
    refresh_requests_list: function() {
        // Ext.MessageBox.alert('Rendered One', 'Tab Two was rendered.');
        // reload
        var panel_requests_list = Ext.getCmp("gPanelRequestsList");
        panel_requests_list.getStore().reload();
    },
    // === requests: refresh file list in request ===
    refresh_requests_file_list: function() {
         Ext.MessageBox.alert('Rendered One', 'Tab Two was rendered.');
    },

    // == dataset ==

    build_panel_datasets : function() {
        var me = this;
        console.log(me);
        // TODO
        // + dataset list
        me.build_panel_datasets_list();
        // + file list in dataset
        me.build_panel_files_in_dataset();

        me.panel_datasets = new Ext.create('Ext.panel.Panel', {
              id : "gPanelDatasets",
              floatable : false,
              layout : 'accordion',
              // layout : 'column',
              header : false,
              border : false,
              items : [me.panel_datasets_list, me.panel_files_in_dataset]
            });

        me.main_panel.add([me.panel_datasets]);
    },

    // === dataset: dataset list ===

    create_datastore_datasets_list : function() {
        var me = this;
        console.log(me);
        me.datastore_dataset_list = new Ext.data.JsonStore({
            proxy : {
                type : 'ajax',
                method : 'POST',
                url : GLOBAL.BASE_URL + 'TransferApp/datasetList',
                reader : {
                  type : 'json',
                  root : 'result'
                },
                timeout : 1800000
            },
            autoLoad : true,
            fields : [
                {
                    name: 'id',
                    type: 'int',
                },
                {
                    name: 'owner',
                    type: 'string',
                },
                {
                    name: 'dataset',
                    type: 'string',
                },
            ],
            listeners: {
                load : function(oStore, records, successful, eOpts) {
                  var bResponseOK = (oStore.proxy.reader.rawData["success"] == "true");
                  if (!bResponseOK) {
                    GLOBAL.APP.CF.alert(oStore.proxy.reader.rawData["error"], "info");
                    if (parseInt(oStore.proxy.reader.rawData["total"], 10) == 0) {
                      me.dataStore.removeAll();
                    }
                  } else {
                    console.log(records);
                  }
                },

            },
        });
    },
    build_panel_datasets_list : function() {
        var me = this;
        console.log(me);
        // -> me.datastore_dataset_list
        me.create_datastore_datasets_list();

        var sm = Ext.create('Ext.selection.RowModel');
        me.panel_datasets_list = new Ext.create('Ext.grid.Panel', {
            id: "gPanelDatasetsList",
            store: me.datastore_dataset_list,
            // autoScroll: true,
            // verticalScroller: {
            //     xtype: 'paginggridscroller',
            //     activePrefetch: false
            // },
            columnWidth: .25,
            selModel : sm,
            columns: [
                {
                    text: "id",
                    dataIndex: "id",
                },
                {
                    text: "name",
                    dataIndex: "dataset",
                },
                {
                    text: "owner",
                    dataIndex: "owner",
                },
            ],
            title: "Datasets list",
            dockedItems: [
                {
                xtype: "toolbar",
                items: [
                {
                    xtype: "button",
                    text: "view",
                    tooltip: "view files in current dataset",
                    // handler: me.view_files_in_current_dataset,
                    handler: function() {
                        console.log(me);
                        var selectedRows = me.panel_datasets_list.getSelectionModel().getSelection();
                        console.log(selectedRows);
                        for (var i = 0; i < selectedRows.length; ++i) {
                            // console.log(selectedRows[i]);
                            // console.log(selectedRows[i].get("id"));
                            var datasetid = selectedRows[i].get("dataset");
                            console.log(datasetid);
                            // after get the datasetid, 
                            // we need to show them in files list.
                            me.datastore_files_in_dataset.load({
                                params: {
                                    dataset: datasetid,
                                },
                                callback: function(records, operation, success) {
                                    // do something after the load finishes
                                    if (success) {
                                        // show the value
                                        me.panel_files_in_dataset.expand();
                                    }
                                },
                            });
                        }

                    }
                },
                {
                    // import button
                    // * import DFC dataset
                    xtype: "button",
                    text: "import",
                    tooltip: "import files list in DFC dataset",
                    handler: function() {
                        // top windows contained:
                        // + top button
                        // + left: dataset
                        // + right: file list
                        me.build_panel_import_dfc_dataset();
                    }
                }
                ],
                },
            ],
        });
    },

    create_datastore_files_in_dataset : function() {
        var me = this;
        console.log(me);
        me.datastore_files_in_dataset = new Ext.data.JsonStore({
            proxy : {
                type : 'ajax',
                url : GLOBAL.BASE_URL + 'TransferApp/datasetListFiles',
                reader : {
                  type : 'json',
                  root : 'result'
                },
                method : 'POST',
                timeout : 1800000
            },
            autoLoad : false,
            autoSync : true,
            fields : [
                {
                    name: 'id',
                    type: 'int',
                },
                {
                    name: 'file',
                    type: 'string',
                },
            ],
            listeners: {
                load : function(oStore, records, successful, eOpts) {
                  var bResponseOK = (oStore.proxy.reader.rawData["success"] == "true");
                  if (!bResponseOK) {
                    GLOBAL.APP.CF.alert(oStore.proxy.reader.rawData["error"], "info");
                    if (parseInt(oStore.proxy.reader.rawData["total"], 10) == 0) {
                      me.dataStore.removeAll();
                    }
                  } else {
                    console.log(records);
                  }
                },

            },
        });
    },
    build_panel_files_in_dataset : function() {
        var me = this;
        console.log(me);
        // -> me.datastore_files_in_dataset
        me.create_datastore_files_in_dataset();
        me.panel_files_in_dataset = new Ext.create('Ext.grid.Panel', {
            id: "gPanelFilesInDatasets",
            // autoScroll: true,
            // verticalScroller: {
            //     xtype: 'paginggridscroller',
            //     activePrefetch: false
            // },
            store: me.datastore_files_in_dataset,
            columnWidth: .75,
            columns: [
                {
                    text: "id",
                    dataIndex: "id",
                },
                {
                    text: "file",
                    dataIndex: "file",
                },
            ],
            title: "Files list",
            tools: [
                {
                    xtype: "button",
                    text: "refresh",
                },
            ],
        });
    },
    // === dataset: view request ===
    view_files_in_current_dataset: function() {
        var me = this;
        console.log(me);
        console.log(me);
        // Ext.MessageBox.alert('Rendered One', 'Tab Two was rendered.');
        var selectedRows = me.panel_datasets_list.getSelectionModel().getSelection();
        console.log(selectedRows);
    },
    // === dataset: import DFC datasets ===
    build_panel_import_dfc_dataset: function() {
        // create panel
        // TODO: using DFC dataset interface
        var panel = new Ext.create('Ext.grid.Panel', {
            id: "gPanelDFCDatasetsList", // the datasets name only
            selModel : Ext.create('Ext.selection.RowModel'),
            store: { // DFC dataset datastore
            proxy : {
                type : 'ajax',
                method : 'POST',
                url : GLOBAL.BASE_URL + 'TransferApp/DFCdatasetList',
                reader : {
                  type : 'json',
                  root : 'result'
                },
                timeout : 1800000
            },
            autoLoad : true,
            fields : [
                {
                    name: 'id',
                    type: 'int',
                },
                {
                    name: 'owner',
                    type: 'string',
                },
                {
                    name: 'dataset',
                    type: 'string',
                },
            ],
            listeners: {
                load : function(oStore, records, successful, eOpts) {
                  var bResponseOK = (oStore.proxy.reader.rawData["success"] == "true");
                  if (!bResponseOK) {
                    GLOBAL.APP.CF.alert(oStore.proxy.reader.rawData["error"], "info");
                    if (parseInt(oStore.proxy.reader.rawData["total"], 10) == 0) {
                      me.dataStore.removeAll();
                    }
                  } else {
                    console.log(records);
                  }
                },

            },
            },
            columns: [
                {
                    text: "id",
                    dataIndex: "id",
                },
                {
                    text: "name",
                    dataIndex: "dataset",
                },
                {
                    text: "owner",
                    dataIndex: "owner",
                },
            ],
            title: "Datasets list",
        });
        panel.getSelectionModel().on('selectionchange', function(sm, selectedRows, r) {
            console.log(sm);
            console.log(selectedRows);
            for (var i = 0; i < selectedRows.length; ++i) {
                // console.log(selectedRows[i].get("dataset"));
                // put the name into input box
                var txt = Ext.getCmp("gUserInputDataset");
                txt.setValue(selectedRows[i].get("dataset"));
            }
        });
        var panel_files = new Ext.create('Ext.grid.Panel', {
            id: "gPanelDFCDatasetsListFiles", // the datasets name only
            store: { // DFC dataset datastore
            proxy : {
                type : 'ajax',
                method : 'POST',
                url : GLOBAL.BASE_URL + 'TransferApp/DFCdatasetListFiles',
                reader : {
                  type : 'json',
                  root : 'result'
                },
                timeout : 1800000
            },
            autoLoad : false,
            autoSync : true,
            fields : [
                {
                    name: 'id',
                    type: 'int',
                },
                {
                    name: 'file',
                    type: 'string',
                },
            ],
            listeners: {
                load : function(oStore, records, successful, eOpts) {
                  var bResponseOK = (oStore.proxy.reader.rawData["success"] == "true");
                  if (!bResponseOK) {
                    GLOBAL.APP.CF.alert(oStore.proxy.reader.rawData["error"], "info");
                    if (parseInt(oStore.proxy.reader.rawData["total"], 10) == 0) {
                      me.dataStore.removeAll();
                    }
                  } else {
                    console.log(records);
                  }
                },

            },
            },
            columns: [
                {
                    text: "id",
                    dataIndex: "id",
                },
                {
                    text: "file",
                    dataIndex: "file",
                },
            ],
            title: "Dataset list files",
        });
        // create a window
        Ext.create('Ext.window.Window', {
            id : "gImportDFCDatasetWin",
            title: 'Import DFC dataset',
            height: 200,
            width: 400,
            // layout: 'fit',
            layout : 'accordion',
            items: [panel, panel_files],
            dockedItems: [
            { // toolbar
                xtype: "toolbar",
                items: [
                { // import
                    xtype: "button",
                    text: "import",
                    tooltip: "import selected DFC dataset",
                    handler: function() {
                        Ext.MessageBox.alert('Import', 'Import selected');
                    }
                },
                { // user input
                    xtype: "textfield",
                    name: "dataset",
                    fieldLabel: "Search dataset",
                    allowBlank: true,
                    id: "gUserInputDataset",
                },
                { // search button
                    xtype: "button",
                    text: "search it!",
                    tooltip: "import selected DFC dataset",
                    handler: function() {
                        var txt = Ext.getCmp("gUserInputDataset");
                        var txtvalue = txt.value;
                        // console.log(txt);
                        // Ext.MessageBox.alert('Search', 'Search '+txt.value);
                        // send post
                        var filelists = Ext.getCmp("gPanelDFCDatasetsListFiles");
                        filelists.getStore().load({
                          params: {
                              dataset: txtvalue,
                          },
                          callback: function(records, operation, success) {
                              if (success) {
                                  filelists.expand();
                              }
                          },
                        });
                    }
                },
                ],
            },
            ]
        }).show(); 

    },
});
