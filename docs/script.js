// MUTATORS (https://tabulator.info/docs/6.3/mutators#mutators)
//
const utc_timestamp_to_akt_obj = (epoch_milliseconds) => {
    if (epoch_milliseconds === null || epoch_milliseconds === undefined) {
        return epoch_milliseconds;
    }
    const datetime = luxon.DateTime.fromMillis(epoch_milliseconds, {zone:"America/Anchorage"});
    return datetime;
};
//
// END MUTATORS

// FORMATTERS (https://tabulator.info/docs/6.3/format#format-custom)
//
const html_link = (cell, formatterParams) => {
    const value = cell.getValue();
    if (value === null || value === undefined) {
        return value;
    }
    let url = value;
    if (formatterParams.hasOwnProperty('link_body')) {
        url = formatterParams.link_body.replace("{value}", value);
    }
    let label = value;
    if (formatterParams.hasOwnProperty('label')) {
        label = formatterParams.label
    }
    return '<a href="' + url + '" target="_blank">' + label + '</a>';
};

const nested_tabulator = (cell, formatterParams, onRendered) => {

    let rows = cell.getValue()
    if (rows === null || rows === undefined) {
        return rows;
    }

    // configure elements
    const holderEl = document.createElement("div");
    const tableEl = document.createElement("div");
    holderEl.style.boxSizing = "border-box";
    holderEl.style.padding = "10px 30px 10px 10px";
    holderEl.style.borderTop = "1px solid #333";
    holderEl.style.borderBotom = "1px solid #333";
    holderEl.style.background = "#ddd";
    tableEl.style.border = "1px solid #333";
    holderEl.appendChild(tableEl);

    // handling field types which specify feature locations
    // popped and cutoff properties are present to keep track of whether / what / how much data was truncated
    if (rows.hasOwnProperty("popped")) {
        if (rows["popped"] > 0) {
            // every interior feature has "dir" === "Interior", all nearest features have "dir" === {cardinal direction}
            if (rows["features"][0]["dir"] === "Interior") {
                holderEl.title = `${rows["popped"]} feature locations deeper than ${rows["cutoff"]} miles interior from the fires edge are not specified.`;
            } else {
                holderEl.title = `${rows["popped"]} feature locations between ${rows["cutoff"]} and 5 miles away from the fires edge are not specified`;
            }
        }
        rows = rows["features"]
    }

    // add configured elements to cell
    cell.getElement().appendChild(holderEl);

    // define tabulator to create when main table renders
    onRendered(function(){

        const table = new Tabulator(tableEl, {
            data:rows,
            layout: formatterParams.layout,
            pagination: formatterParams.pagination,
            paginationSize: formatterParams.paginationSize,
            paginationCounter: formatterParams.paginationCounter,
            movableColumns: formatterParams.movableColumns,
            resizableRows: formatterParams.resizableRows,
            columns: formatterParams.columns
        })
    })

    // add details element to allow expansion of nested tabulator
    const tableDropDown = document.createElement("details");
    const summary = document.createElement("summary");
    summary.innerHTML = "<strong>Click to Expand</strong>";
    tableDropDown.appendChild(summary);
    tableDropDown.appendChild(holderEl);

    return tableDropDown
}
//
// END FORMATTERS

// HEADER FILTER FUNCTIONS (https://tabulator.info/docs/6.3/filter#header)
//
const list_autocomplete_multi_header_filter = (headerValue, rowValue, rowData, filterParams) => {

    // convert null or undefined rowValue to empty string, because string methods get called on rowValue prior to return
    rowValue = _null_to_empty(rowValue);

    // conditions under which every rowValue is considered valid (which effectively means unfiltering the column)
    if (headerValue === null || headerValue === undefined) {
        return true;
    }
    if (headerValue.trim() === "") {
        return true;
    }

    // get array of all values (comma seperated) entered into header filter, ignoring capitalization and trimming whitespace
    let header_values = headerValue.split(',');
    header_values = header_values.map(value => value.trim().toLowerCase());

    // any row value that begins with the text of a header filter value (ignoring capitalization) is considered valid
    return (header_values.some(hv => rowValue.toLowerCase().startsWith(hv)));
}

const _null_to_empty = (value) => (value === null || value === undefined) ? "" : value;
//
// END HEADER FILTER FUNCTIONS

// HEADER MENUS (https://tabulator.info/docs/6.3/menu#header-menu)
//
const header_menu = function(){

    // to be returned
    const menu = [];

    // to dictate ordering of columns in menu (for groups, using parent columns instead of children)
    const columns = this.getColumns(true);

    // to determine which columns should be specified in menu
    const columns_with_data = new Set();

    // to be used for identifying empty child fields (empty fields that are part of column group)
    // some fields included by default to prevent group column header collapse
    const child_fields_with_data = new Set ([
        "MgmtOption_AcreSum.Critical",
        "MgmtOption_AcreSum.Full",
        "MgmtOption_AcreSum.Limited",
        "Jurisd_Owner_AcreSum.Private",
        "Jurisd_Owner_AcreSum.State",
    ]);


    // to retrieve empty child column objects that need to be re-hidden after a parent column is turned on
    const columns_including_children = this.getColumns();

    // populate sets based on what data is present in the current table
    const rows = this.getRows("all");
    for (let row of rows) {
        const cells = row.getCells();
        for (let cell of cells) {
            if (cell.getValue() != null && cell.getValue() != undefined) {
                const col = cell.getColumn();
                const parent = col.getParentColumn();
                if (parent === false) {
                    columns_with_data.add(col);
                } else {
                    columns_with_data.add(parent);
                    child_fields_with_data.add(col.getField());
                }
            }
        }
    }
    
    // create a Hide All Columns menu button
    const hide_icon = document.createElement("i");
    hide_icon.classList.add("fas", "fa-eye-slash");
    const hide_label = document.createElement("span");
    const hide_title = document.createElement("span");
    hide_title.innerHTML = '<b> Hide All Columns</b>';
    hide_label.appendChild(hide_icon);
    hide_label.appendChild(hide_title);
    menu.push({
        label:hide_label,
        action:function(e) {
            for (let column of columns) {
                if (column.getField() === "AkFireNumber") {
                    continue;
                }
                if (!columns_with_data.has(column)) {
                    continue;
                }
                column.hide();
            }
        }
    });

    // create a Show All Columns menu button
    const show_icon = document.createElement("i");
    show_icon.classList.add("fas", "fa-eye");
    const show_label = document.createElement("span");
    const show_title = document.createElement("span");
    show_title.innerHTML = "<b> Show All Columns</b>"
    show_label.appendChild(show_icon);
    show_label.appendChild(show_title);
    menu.push({
        label:show_label,
        action:function(e) {
            for (let column of columns) {
                if (column.getField() === "AkFireNumber") {
                    continue;
                }
                if (!columns_with_data.has(column)) {
                    continue;
                }
                column.show();
                // re-hiding child columns that have no data 
                if (column.getDefinition().hasOwnProperty("columns")) {
                    const children = column.getDefinition().columns;
                    for (let child of children) {
                        if (!child_fields_with_data.has(child.field)) {
                            const child_to_hide = columns_including_children.find(col => col.getField() === child.field);
                            setTimeout(() => {child_to_hide.hide()}, 100);
                        }
                    }
                }
            }
        }
    });

    // create menu buttons to toggle specific columns / group columns on and off
    for (let column of columns) {
        if (column.getField() === "AkFireNumber") {
            continue;
        }
        if (!columns_with_data.has(column)) {
            continue;
        }
        let icon = document.createElement("i");
        icon.classList.add("fas");
        icon.classList.add(column.isVisible() ? "fa-check-square" : "fa-square");
        let label = document.createElement("span");
        let title = document.createElement("span");
        title.textContent = " " + column.getDefinition().title;
        label.appendChild(icon);
        label.appendChild(title);
        menu.push({
            label:label,
            action:function(e) {
                e.stopPropagation();
                column.toggle();
                if (column.isVisible()){
                    icon.classList.remove("fa-square");
                    icon.classList.add("fa-check-square");

                    // re-hiding child columns that have no data 
                    if (column.getDefinition().hasOwnProperty("columns")) {
                        const children = column.getDefinition().columns;
                        for (let child of children) {
                            if (!child_fields_with_data.has(child.field)) {
                                const child_to_hide = columns_including_children.find(col => col.getField() === child.field);
                                setTimeout(() => {child_to_hide.hide()}, 100);
                            }
                        }
                    }
                } else{
                    icon.classList.remove("fa-check-square");
                    icon.classList.add("fa-square");
                }
            }
        });
    }
    return menu;
};
//
// END HEADER MENUS

// MAIN TABULATOR
//
const build_table = (tag, rows_array) => {
    const interior_or_nearest = (tag === "#perimeters-and-locations") ? "Nearest Features (Inside Perimeter)" : "Nearest Features (Outside Perimeter)"
    const table = new Tabulator(tag, {
        // persistence has pros and cons. look into options for resetting table state.
        /*
        persistence:{ 
            sort: true, 
            headerFilter: true, 
            columns: true, 
        },
        persistenceID: `pid_${tag.slice(1)}`,
        */
        height:"100%",  
        layout: "fitData",
        pagination: "local",
        paginationSize: 20, 
        paginationCounter: "rows",
        movableColumns: true, 
        resizableRows: true,
        data: rows_array,
        headerFilterLiveFilterDelay:900,
        columnDefaults:{
            tooltip:function(e, cell, onRendered){
                const val = cell.getValue();
                if (val === '!error!' || val === -1){
                    const el = document.createElement("div");
                    el.style.backgroundColor = "red";
                    el.innerHTML = '<p style="font-size: 12pt; font-weight: bold;">An error occurred. Data is not valid. Check back later.</p>';
                    return el;
                }
            }
        },
        columns: [
            {title:"Fire Number", field:"AkFireNumber", frozen:true, headerFilter:"list", headerFilterParams:{
                valuesLookup:"active",
                sort:'desc',
                autocomplete:true,
                allowEmpty:true,
                listOnEmpty:true
            }, headerFilterFunc:list_autocomplete_multi_header_filter,
            headerMenu:header_menu, headerMenuIcon:"<i class='fas fa-bars'></i>"},
            {title:"Fire Name", field:"wfigs_IncidentName", frozen:true, headerFilter:"list", headerFilterParams:{
                valuesLookup:"active",
                sort:'asc',
                autocomplete:true,
                allowEmpty:true,
                listOnEmpty:true
            }, headerFilterFunc:list_autocomplete_multi_header_filter},
            {title:"Spatial Information Type", field:"SpatialInfoType", frozen:true, headerSort:false},
            {title:"Web App Link", field:"VarAppURL", frozen:true, formatter:html_link, formatterParams:{
                label:"View on Map"
            }, headerSort:false},
            {title:"Activity Status", field:"FireActivityStatus", headerFilter:"list", headerFilterParams:{
                valuesLookup:"active",
                sort:'asc',
                autocomplete:true,
                allowEmpty:true,
                listOnEmpty:true
            }, headerFilterFunc:list_autocomplete_multi_header_filter},
            {title:"Percent Contained", field:"PercentContained", formatter:"money", formatterParams:{
                symbol:"%",
                symbolAfter:true,
                precision:1,

            }},
            {title:"Percent Perimeter to be Contained", field:"PercentPerimeterToBeContained", formatter:"money", formatterParams:{
                symbol:"%",
                symbolAfter:true,
                precision:1,

            }},
            {title:"Total Incident Personnel", field:"TotalIncidentPersonnel", formatter:"money", formatterParams:{
                precision:0
            }, topCalc: "sum", topCalcParams:{
                precision:0,
            }},
            {title:"Estimated Cost to Date", field:"EstimatedCostToDate", topCalc: "sum", topCalcParams:{
                precision:0,
            }, formatter:"money", formatterParams:{
                symbol:"$",
                precision:0
            }, topCalcFormatter:"money", topCalcFormatterParams:{
                symbol:"$",
                precision:0
            }},
            {title:"Complex Name", field:"CpxName", headerFilter:"list", headerFilterParams:{
                valuesLookup:"active",
                sort:'asc',
                autocomplete:true,
                allowEmpty:true,
                listOnEmpty:true
            }, headerFilterFunc:list_autocomplete_multi_header_filter},
            {title:"Fire Management Complexity", field:"FireMgmtComplexity", headerFilter:"list", headerFilterParams:{
                valuesLookup:"active",
                sort:'asc',
                autocomplete:true,
                allowEmpty:true,
                listOnEmpty:true
            }, headerFilterFunc:list_autocomplete_multi_header_filter},
            {title:"Incident Complexity Level", field:"IncidentComplexityLevel", headerFilter:"list", headerFilterParams:{
                valuesLookup:"active",
                sort:'asc',
                autocomplete:true,
                allowEmpty:true,
                listOnEmpty:true
            }, headerFilterFunc:list_autocomplete_multi_header_filter},
            {title:"Incident Management Organization", field:"IncidentManagementOrganization", headerFilter:"list", headerFilterParams:{
                valuesLookup:"active",
                sort:'asc',
                autocomplete:true,
                allowEmpty:true,
                listOnEmpty:true
            }, headerFilterFunc:list_autocomplete_multi_header_filter},
            {title:"ICS 209 Report Status", field:"ICS209ReportStatus", headerFilter:"list", headerFilterParams:{
                valuesLookup:"active",
                sort:'asc',
                autocomplete:true,
                allowEmpty:true,
                listOnEmpty:true
            }, headerFilterFunc:list_autocomplete_multi_header_filter},
            {title:"ICS 209 Report Date Time", field:"ICS209ReportDateTime", mutator:utc_timestamp_to_akt_obj, formatter:"datetime", formatterParams:{
                outputFormat:"yyyy-MM-dd HH:mm:ss"
            }, sorter:"datetime", sorterParams:{
                format:"yyyy-MM-dd HH:mm:ss",
                alignEmptyValues:"bottom"
            }},
            {title:"Dispatch Center", field:"DispatchCenterID", headerFilter:"list", headerFilterParams:{
                valuesLookup:"active",
                sort:'asc',
                autocomplete:true,
                allowEmpty:true,
                listOnEmpty:true
            }, headerFilterFunc:list_autocomplete_multi_header_filter},
            {title:"Region", field:"AkFireRegion", headerFilter:"list", headerFilterParams:{
                valuesLookup:"active",
                sort:'asc',
                autocomplete:true,
                allowEmpty:true,
                listOnEmpty:true
            }, headerFilterFunc:list_autocomplete_multi_header_filter},
            {title:"POO Protecting Agency", field:"POOProtectingAgency", headerFilter:"list", headerFilterParams:{
                valuesLookup:"active",
                sort:'asc',
                autocomplete:true,
                allowEmpty:true,
                listOnEmpty:true
            }, headerFilterFunc:list_autocomplete_multi_header_filter},
            {title:"POO Jurisdictional Agency", field:"POOJurisdictionalAgency", headerFilter:"list", headerFilterParams:{
                valuesLookup:"active",
                sort:'asc',
                autocomplete:true,
                allowEmpty:true,
                listOnEmpty:true
            }, headerFilterFunc:list_autocomplete_multi_header_filter},
            {title:"POO Jurisdictional Unit", field:"POOJurisdictionalUnit", headerFilter:"list", headerFilterParams:{
                valuesLookup:"active",
                sort:'asc',
                autocomplete:true,
                allowEmpty:true,
                listOnEmpty:true
            }, headerFilterFunc:list_autocomplete_multi_header_filter},
            {title:"Jurisdictional Unit Acres", field:"Jurisd_Unit_AcreSum", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                layout: "fitDataStretch",
                pagination: "local",
                paginationSize: 5, 
                paginationCounter: "rows",
                movableColumns: true, 
                resizableRows: true,
                columns:[
                    {title:"Jurisdictional Unit", field:"key head"},
                    {title:"Acres", field: "value head", formatter:"money", formatterParams:{precision:2}}
                ]
            },    
                headerSort:false},
            {title:"Information Last Updated (AKT)", field:"wfigs_ModifiedOnDateTime_dt", mutator:utc_timestamp_to_akt_obj, formatter:"datetime", formatterParams:{
                outputFormat:"yyyy-MM-dd HH:mm:ss"
            }, sorter:"datetime", sorterParams:{
                format:"yyyy-MM-dd HH:mm:ss",
                alignEmptyValues:"bottom"
            }},
            {title:"Reported Acres", field:"ReportedAcres", formatter:"money", formatterParams:{
                precision:2
            }, topCalc: "sum", topCalcParams:{
                precision:2
            }, topCalcFormatter:"money", topCalcFormatterParams:{
                precision:2
            }},
            {title:"GIS Acres", field:"wfigs_GISAcres", formatter:"money", formatterParams:{
                precision:2
            }, topCalc: "sum", topCalcParams:{
                precision:2
            }, topCalcFormatter:"money", topCalcFormatterParams:{
                precision:2
            }},
            {title:"Map Method", field:"wfigs_MapMethod", headerFilter:"list", headerFilterParams:{
                valuesLookup:"active",
                sort:'asc',
                autocomplete:true,
                allowEmpty:true,
                listOnEmpty:true
            }, headerFilterFunc:list_autocomplete_multi_header_filter},
            {title:"Perimeter Last updated (AKT)", field:"wfigs_PolygonDateTime", mutator:utc_timestamp_to_akt_obj, formatter:"datetime", formatterParams:{
                outputFormat:"yyyy-MM-dd HH:mm:ss"
            }, sorter:"datetime", sorterParams:{
                format:"yyyy-MM-dd HH:mm:ss",
                alignEmptyValues:"bottom"
            }},
            {
                title:"Management Option Acres",
                headerHozAlign:"center",
                columns:[
                    {title:"Critical", field:"MgmtOption_AcreSum.Critical", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }},
                    {title:"Full", field:"MgmtOption_AcreSum.Full", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }},
                    {title:"Limited", field:"MgmtOption_AcreSum.Limited", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }},
                    {title:"Mod. (Jul 10)", field:"MgmtOption_AcreSum.Modified (Jul 10)", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }},
                    {title:"Mod. (Aug 10)", field:"MgmtOption_AcreSum.Modified (Aug 10)", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }},
                    {title:"Mod. (Aug 20)", field:"MgmtOption_AcreSum.Modified (Aug 20)", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }},
                    {title:"Mod. (Sep 30)", field:"MgmtOption_AcreSum.Modified (Sep 30)", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }},
                    {title:"Unplanned", field:"MgmtOption_AcreSum.Unplanned", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }}
                ]
            },
            {
                title:"Ownership Acres",
                headerHozAlign:"center",
                columns:[
                    {title:"ANCSA", field:"Jurisd_Owner_AcreSum.ANCSA", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }},
                    {title:"BIA", field:"Jurisd_Owner_AcreSum.BIA", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }},
                    {title:"BLM", field:"Jurisd_Owner_AcreSum.BLM", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }},
                    {title:"City", field:"Jurisd_Owner_AcreSum.City", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }},
                    {title:"County", field:"Jurisd_Owner_AcreSum.County", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }},
                    {title:"DOD", field:"Jurisd_Owner_AcreSum.DOD", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }},
                    {title:"DOE", field:"Jurisd_Owner_AcreSum.DOE", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }},
                    {title:"NPS", field:"Jurisd_Owner_AcreSum.NPS", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }},
                    {title:"OthFed", field:"Jurisd_Owner_AcreSum.OthFed", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }},
                    {title:"Private", field:"Jurisd_Owner_AcreSum.Private", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }},
                    {title:"State", field:"Jurisd_Owner_AcreSum.State", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }},
                    {title:"Tribal", field:"Jurisd_Owner_AcreSum.Tribal", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }},
                    {title:"USFS", field:"Jurisd_Owner_AcreSum.USFS", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }},
                    {title:"USFWS", field:"Jurisd_Owner_AcreSum.USFWS", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }},
                ]
            },
            {
                title:"Weather Stations",
                headerHozAlign:"center",
                columns:[
                    {title:"Feature Count", field:"WeatherStation_FeatureCount", formatter:"money", formatterParams:{
                        precision:0
                    }, topCalc: "sum", topCalcParams:{
                        precision:0,
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:0
                    }},
                    {title:interior_or_nearest, field:"WeatherStation_Locations", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Name", field:"NAME"},
                            {title:"Code", field:"CODE"},
                            {title:"URL", field:"MESOWESTWEBURL", formatter:html_link, formatterParams:{
                                label:"MesoWest CFFDRS"
                            }},
                            {title:"Miles from Fire Edge", field:"dist_mi", formatter:"money", formatterParams:{precision:2}},
                            {title:"Direction", field:"dir"},
                            {title:"Latitude", field:"lat"},
                            {title:"Longitude", field:"lng"}
                        ]
                    }, headerSort:false},
                ]
            },
            {
                title:"Runways",
                headerHozAlign:"center",
                columns:[
                    {title:"Feature Count", field:"Runway_FeatureCount", formatter:"money", formatterParams:{
                        precision:0
                    }, topCalc: "sum", topCalcParams:{
                        precision:0,
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:0
                    }}, 
                    {title:interior_or_nearest, field:"Runway_Locations", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Location ID", field:"loc_id"},
                            {title:"Runway ID", field:"runway_id"},
                            {title:"Length", field:"length"},
                            {title:"Surface", field:"surface"},
                            {title:"AirNav URL", field:"loc_id", formatter:html_link, formatterParams:{
                                link_body:"https://www.airnav.com/airport/{value}",
                                label:"Go to AirNav"
                            }},
                            {title:"WeatherCams URL", field:"loc_id", formatter:html_link, formatterParams:{
                                link_body:"https://weathercams.faa.gov/map/airport/{value}/details/camera",
                                label:"Go to WeatherCams"
                            }},
                            {title:"Miles from Fire Edge", field:"dist_mi", formatter:"money", formatterParams:{precision:2}},
                            {title:"Direction", field:"dir"},
                            {title:"Latitude", field:"lat"},
                            {title:"Longitude", field:"lng"}
                        ]
                    },    
                        headerSort:false},
                    {title:"Count by Ownership", field:"Runway_Ownership_AttrCount", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Ownership", field:"key head"},
                            {title:"Feature Count", field: "value head", formatter:"money", formatterParams:{precision:0}}
                        ]
                    }, headerSort:false},
                ]
            },
            {
                title:"Communities",
                headerHozAlign:"center",
                columns:[
                    {title:"Feature Count", field:"Community_FeatureCount", formatter:"money", formatterParams:{
                        precision:0
                    }, topCalc: "sum", topCalcParams:{
                        precision:0,
                    }},
                    {title:interior_or_nearest, field:"Community_Locations", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Community", field:"CommunityName"},
                            {title:"Miles from Fire Edge", field:"dist_mi", formatter:"money", formatterParams:{precision:2}},
                            {title:"Direction", field:"dir"},
                            {title:"Latitude", field:"lat"},
                            {title:"Longitude", field:"lng"}
                        ]
                    }, headerSort:false},
                ]
            },
            {
                title:"USA Structures (FEMA)",
                headerHozAlign:"center",
                columns:[
                    {title:"Feature Count", field:"UsaStruct_FeatureCount", formatter:"money", formatterParams:{
                        precision:0
                    }, topCalc: "sum", topCalcParams:{
                        precision:0,
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:0
                    }},
                    {title:interior_or_nearest, field:"UsaStruct_Locations", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Occupational Class", field:"OCC_CLS"},
                            {title:"Primary Occupation", field:"PRIM_OCC"},
                            {title:"Miles from Fire Edge", field:"dist_mi", formatter:"money", formatterParams:{precision:2}},
                            {title:"Direction", field:"dir"},
                            {title:"Latitude", field:"lat"},
                            {title:"Longitude", field:"lng"}
                        ]
                    }, headerSort:false},
                    {title:"Occupational Class", field:"UsaStruct_OccCls_AttrCount", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Occupational Class", field:"key head"},
                            {title:"Structure Count", field: "value head", formatter:"money", formatterParams:{precision:0}}
                        ]
                    },    
                        headerSort:false}, 
                    {title:"Primary Occupation", field:"UsaStruct_PrimOcc_AttrCount", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Primary Occupation", field:"key head"},
                            {title:"Structure Count", field: "value head", formatter:"money", formatterParams:{precision:0}}
                        ]
                    },    
                        headerSort:false},
                    {title:"Secondary Occupation", field:"UsaStruct_SecOcc_AttrCount", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Secondary Occupation", field:"key head"},
                            {title:"Structure Count", field: "value head", formatter:"money", formatterParams:{precision:0}}
                        ]
                    },    
                        headerSort:false}, 
                ]
            },
            {
                title:"Known Sites Database",
                headerHozAlign:"center",
                columns:[
                    {title:"Feature Count", field:"Aksd_FeatureCount", formatter:"money", formatterParams:{
                        precision:0
                    }, topCalc: "sum", topCalcParams:{
                        precision:0,
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:0
                    }},
                    {title:interior_or_nearest, field:"Aksd_Locations", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Site Category", field:"SiteCat"},
                            {title:"Protection Level", field:"Protection"},
                            {title:"Main Structures", field:"MainStructs"},
                            {title:"Other Structures", field:"OthrStructs"},
                            {title:"Miles from Fire Edge", field:"dist_mi", formatter:"money", formatterParams:{precision:2}},
                            {title:"Direction", field:"dir"},
                            {title:"Latitude", field:"lat"},
                            {title:"Longitude", field:"lng"}
                        ]
                    }, headerSort:false},
                    {title:"Main Structures Count", field:"Aksd_MainStruct_AttrSum", formatter:"money", formatterParams:{
                        precision:0
                    }, topCalc: "sum", topCalcParams:{
                        precision:0,
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:0
                    }},                    
                    {title:"Other Structures Count", field:"Aksd_OtherStruct_AttrSum", formatter:"money", formatterParams:{
                        precision:0
                    }, topCalc: "sum", topCalcParams:{
                        precision:0,
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:0
                    }},                        
                    {title:"Protection Levels", field:"Aksd_Protection_AttrCount", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Protection Levels", field:"key head"},
                            {title:"Site Count", field: "value head", formatter:"money", formatterParams:{precision:0}}
                        ]
                    },    
                        headerSort:false}, 
                    {title:"Site Categories", field:"Aksd_SiteCat_AttrCount", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Site Categories", field:"key head"},
                            {title:"Site Count", field: "value head", formatter:"money", formatterParams:{precision:0}}
                        ]
                    },    
                        headerSort:false},  
                ]
            },
            {
                title:"NPS Structures",
                headerHozAlign:"center",
                columns:[
                    {title:"Feature Count", field:"NpsStruct_FeatureCount", formatter:"money", formatterParams:{
                        precision:0
                    }, topCalc: "sum", topCalcParams:{
                        precision:0,
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:0
                    }},
                    {title:interior_or_nearest, field:"NpsStruct_Locations", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Protection Level", field:"ProtectionLevel"},
                            {title:"Structure Use", field:"FacilityUse"},
                            {title:"Risk Rating", field:"Rating"},
                            {title:"Miles from Fire Edge", field:"dist_mi", formatter:"money", formatterParams:{precision:2}},
                            {title:"Direction", field:"dir"},
                            {title:"Latitude", field:"lat"},
                            {title:"Longitude", field:"lng"}
                        ]
                    }, headerSort:false},
                    {title:"Protection Levels", field:"NpsStruct_Protection_AttrCount", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Protection Levels", field:"key head"},
                            {title:"Structure Count", field: "value head", formatter:"money", formatterParams:{precision:0}}
                        ]
                    },    
                        headerSort:false},  
                    {title:"Structure Use", field:"NpsStruct_Use_AttrCount", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Structure Use", field:"key head"},
                            {title:"Structure Count", field: "value head", formatter:"money", formatterParams:{precision:0}}
                        ]
                    },    
                        headerSort:false},  
                    {title:"Risk Rating", field:"NpsStruct_Rating_AttrCount", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Risk Rating", field:"key head"},
                            {title:"Structure Count", field: "value head", formatter:"money", formatterParams:{precision:0}}
                        ]
                    },    
                        headerSort:false},
                ]
            },
            {
                title:"Parcels",
                headerHozAlign:"center",
                columns:[
                    {title:"Feature Count", field:"Parcel_FeatureCount", formatter:"money", formatterParams:{
                        precision:0
                    }, topCalc: "sum", topCalcParams:{
                        precision:0,
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:0
                    }},
                    {title:interior_or_nearest, field:"Parcel_Locations", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Property Type", field:"property_type"},
                            {title:"Property Use", field:"property_use"},
                            {title:"Total Value", field:"total_value", formatter:"money", formatterParams:{
                                symbol:"$",
                                precision:0
                            }},
                            {title:"Miles from Fire Edge", field:"dist_mi", formatter:"money", formatterParams:{precision:2}},
                            {title:"Direction", field:"dir"},
                            {title:"Latitude", field:"lat"},
                            {title:"Longitude", field:"lng"}
                        ]
                    }, headerSort:false},
                    {title:"Land Value", field:"Parcel_LandValue_AttrSum", topCalc: "sum", topCalcParams:{
                        precision:0,
                    }, formatter:"money", formatterParams:{
                        symbol:"$",
                        precision:0
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        symbol:"$",
                        precision:0
                    }},
                    {title:"Building Value", field:"Parcel_BuildingValue_AttrSum", topCalc: "sum", topCalcParams:{
                        precision:0,
                    }, formatter:"money", formatterParams:{
                        symbol:"$",
                        precision:0
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        symbol:"$",
                        precision:0
                    }},
                    {title:"Total Value", field:"Parcel_TotalValue_AttrSum", topCalc: "sum", topCalcParams:{
                        precision:0,
                    }, formatter:"money", formatterParams:{
                        symbol:"$",
                        precision:0
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        symbol:"$",
                        precision:0
                    }},
                    {title:"Total Parcel Acres", field:"Parcel_TotalAcres", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }},
                    {title:"Acres by Property Type", field:"Parcel_PropType_AcreSum", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Property Type", field:"key head"},
                            {title:"Acres", field: "value head", formatter:"money", formatterParams:{precision:1}}
                        ]
                    },    
                        headerSort:false},
                    {title:"Acres by Property Use", field:"Parcel_PropUse_AcreSum", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Property Use", field:"key head"},
                            {title:"Acres", field: "value head", formatter:"money", formatterParams:{precision:1}}
                        ]
                    },    
                        headerSort:false},
                ]
            },
            {
                title:"Native Allotments",
                headerHozAlign:"center",
                columns:[
                    {title:"Feature Count", field:"NtvAllot_FeatureCount", formatter:"money", formatterParams:{
                        precision:0
                    }, topCalc: "sum", topCalcParams:{
                        precision:0,
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:0
                    }},
                    {title:interior_or_nearest, field:"NtvAllot_Locations", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Status", field:"ALLOT_STATUS"},
                            {title:"ID", field:"ALLOT_ID"},
                            {title:"Miles from Fire Edge", field:"dist_mi", formatter:"money", formatterParams:{precision:2}},
                            {title:"Direction", field:"dir"},
                            {title:"Latitude", field:"lat"},
                            {title:"Longitude", field:"lng"}
                        ]
                    }, headerSort:false},
                    {title:"Total Allotment Acres", field:"NtvAllot_TotalAcres", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }},
                    {title:"Restricted Allotment Acres", field:"NtvAllot_Status_AcreSum.Restricted", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }},
                    {title:"Pending Allotment Acres", field:"NtvAllot_Status_AcreSum.Pending", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }},
                ]
    
            },
            {
                title:"Powerlines",
                headerHozAlign:"center",
                columns:[
                    {title:"Total Feet", field:"AkPowerLine_TotalFeet", formatter:"money", formatterParams:{
                        precision:0
                    }, topCalc: "sum", topCalcParams:{
                        precision:0,
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:0
                    }},
                    {title:interior_or_nearest, field:"AkPowerLine_Locations", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Conduction Type", field:"COND_TYPE"},
                            {title:"Voltage", field:"VOLTAGE"},
                            {title:"Miles from Fire Edge", field:"dist_mi", formatter:"money", formatterParams:{precision:2}},
                            {title:"Direction", field:"dir"},
                            {title:"Latitude", field:"lat"},
                            {title:"Longitude", field:"lng"}
                        ]
                    }, headerSort:false},
                    {title:"Feet by Conduction Type", field:"AkPowerLine_CondType_FeetSum", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Conduction Type", field:"key head"},
                            {title:"Feet", field: "value head", formatter:"money", formatterParams:{precision:0}}
                        ]
                    },    
                        headerSort:false},
                    {title:"Feet by Voltage", field:"AkPowerLine_Voltage_FeetSum", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Voltage", field:"key head"},
                            {title:"Feet", field: "value head", formatter:"money", formatterParams:{precision:0}}
                        ]
                    },    
                        headerSort:false},
                ]
            },
            {
                title:"Power Plants",
                headerHozAlign:"center",
                columns:[
                    {title:"Feature Count", field:"PowerPlant_FeatureCount", formatter:"money", formatterParams:{
                        precision:0
                    }, topCalc: "sum", topCalcParams:{
                        precision:0,
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:0
                    }},
                    {title:interior_or_nearest, field:"PowerPlant_Locations", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Utility Name", field:"Utility_Name"},
                            {title:"Sector Name", field:"Sector_Name"},
                            {title:"Address", field:"Street_Address"},
                            {title:"Source", field:"PrimSource"},
                            {title:"Description", field:"tech_desc"},
                            {title:"Miles from Fire Edge", field:"dist_mi", formatter:"money", formatterParams:{precision:2}},
                            {title:"Direction", field:"dir"},
                            {title:"Latitude", field:"lat"},
                            {title:"Longitude", field:"lng"}
                        ]
                    }, headerSort:false},
                    {title:"Power Source", field:"PowerPlant_PrimSource_AttrCount", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Power Source", field:"key head"},
                            {title:"Feature Count", field: "value head", formatter:"money", formatterParams:{precision:0}}
                        ]
                    }, headerSort:false},
                    {title:"Sector Name", field:"PowerPlant_SectorName_AttrCount", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Sector Name", field:"key head"},
                            {title:"Feature Count", field: "value head", formatter:"money", formatterParams:{precision:0}}
                        ]
                    }, headerSort:false},
                    {title:"Utility Name", field:"PowerPlant_UtilName_AttrCount", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Utility Name", field:"key head"},
                            {title:"Feature Count", field: "value head", formatter:"money", formatterParams:{precision:0}}
                        ]
                    }, headerSort:false},
                ]
            },
            {
                title:"Communication Sites",
                headerHozAlign:"center",
                columns:[
                    {title:"Feature Count", field:"ComsTwr_FeatureCount", formatter:"money", formatterParams:{
                        precision:0
                    }, topCalc: "sum", topCalcParams:{
                        precision:0,
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:0
                    }},
                    {title:interior_or_nearest, field:"ComsTwr_Locations", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Type", field:"Type"},
                            {title:"Details", field:"url", formatter:html_link, formatterParams:{
                                label:"Go to webpage"
                            }},
                            {title:"Miles from Fire Edge", field:"dist_mi", formatter:"money", formatterParams:{precision:2}},
                            {title:"Direction", field:"dir"},
                            {title:"Latitude", field:"lat"},
                            {title:"Longitude", field:"lng"}
                        ]
                    }, headerSort:false},
                    {title:"Site Type", field:"ComsTwr_Type_AttrCount", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Site Type", field:"key head"},
                            {title:"Feauture Count", field: "value head", formatter:"money", formatterParams:{precision:0}}
                        ]
                    }, headerSort:false}
                ]
            },
            {
                title:"Pipelines",
                headerHozAlign:"center",
                columns:[
                    {title:"Total Feet", field:"PipeLine_TotalFeet", formatter:"money", formatterParams:{
                        precision:0
                    }, topCalc: "sum", topCalcParams:{
                        precision:0,
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:0
                    }},
                    {title:interior_or_nearest, field:"PipeLine_Locations", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Name", field:"NAME"},
                            {title:"Miles from Fire Edge", field:"dist_mi", formatter:"money", formatterParams:{precision:2}},
                            {title:"Direction", field:"dir"},
                            {title:"Latitude", field:"lat"},
                            {title:"Longitude", field:"lng"}
                        ]
                    }, headerSort:false},
                    {title:"Feet by Name", field:"PipeLine_Name_FeetSum", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Pipeline Name", field:"key head"},
                            {title:"Feet", field: "value head", formatter:"money", formatterParams:{precision:0}}
                        ]
                    }, headerSort:false}
                ]
            },
            {
                title:"Petrol Terminals",
                headerHozAlign:"center",
                columns:[
                    {title:"Feature Count", field:"PetrolTerm_FeatureCount", formatter:"money", formatterParams:{
                        precision:0
                    }, topCalc: "sum", topCalcParams:{
                        precision:0,
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:0
                    }},  
                    {title:interior_or_nearest, field:"PetrolTerm_Locations", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Company", field:"Company"},
                            {title:"Site", field:"Site"},
                            {title:"City", field:"City"},
                            {title:"Miles from Fire Edge", field:"dist_mi", formatter:"money", formatterParams:{precision:2}},
                            {title:"Direction", field:"dir"},
                            {title:"Latitude", field:"lat"},
                            {title:"Longitude", field:"lng"}
                        ]
                    }, headerSort:false},
                    {title:"Count by Company", field:"PetrolTerm_Company_AttrCount", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Company Name", field:"key head"},
                            {title:"Feature Count", field: "value head", formatter:"money", formatterParams:{precision:0}}
                        ]
                    }, headerSort:false}
                ]
            },
            {
                title:"Mines",
                headerHozAlign:"center",
                columns:[
                    {title:"Feature Count", field:"AkMine_FeatureCount", formatter:"money", formatterParams:{
                        precision:0
                    }, topCalc: "sum", topCalcParams:{
                        precision:0,
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:0
                    }},  
                    {title:interior_or_nearest, field:"AkMine_Locations", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Activity Status", field:"ACTIVE", formatter:function(cell, formatterParams, onRendered){
                                const value = cell.getValue();
                                if (value === "No Data") {
                                    return value
                                }
                                if (![0,1].includes(value)) {
                                    return null
                                }
                                const alias = (value === 0) ? "Non-active" : "Active";
                                return alias
                            }},
                            {title:"Owner", field:"NOTE"},
                            {title:"Mining Type", field:"APMA_TYPE"},
                            {title:"Explosives on Site", field:"Explosives", formatter:function(cell, formatterParams, onRendered){
                                const value = cell.getValue();
                                if (value === "No Data") {
                                    return value
                                }
                                if (![0,1].includes(value)) {
                                    return null
                                }
                                const alias = (value === 0) ? "No" : "Yes";
                                return alias
                            }},
                            {title:"Start Year", field:"START_YEAR"},
                            {title:"Expiration Year", field:"EXPIRATION_YEAR"},
                            {title:"Miles from Fire Edge", field:"dist_mi", formatter:"money", formatterParams:{precision:2}},
                            {title:"Direction", field:"dir"},
                            {title:"Latitude", field:"lat"},
                            {title:"Longitude", field:"lng"}
                        ]
                    }, headerSort:false},
                    {title:"Count by Activity Status", field:"AkMine_Active_AttrCount", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Activity Status", field:"key head", formatter:function(cell, formatterParams, onRendered){
                                const value = cell.getValue();
                                if (!["0","1"].includes(value)) {
                                    return null
                                }
                                const alias = (value === "0") ? "Non-active" : "Active";
                                return alias
                            }},
                            {title:"Feature Count", field: "value head", formatter:"money", formatterParams:{precision:0}}
                        ]
                    }, headerSort:false},
                    {title:"Count by Type", field:"AkMine_Type_AttrCount", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Mining Type", field:"key head"},
                            {title:"Feature Count", field: "value head", formatter:"money", formatterParams:{precision:0}}
                        ]
                    }, headerSort:false},
                    {title:"Count by Owner", field:"AkMine_Note_AttrCount", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Owner", field:"key head"},
                            {title:"Feature Count", field: "value head", formatter:"money", formatterParams:{precision:0}}
                        ]
                    }, headerSort:false}
                ]
            },
            {
                title:"Silviculture",
                headerHozAlign:"center",
                columns:[
                    {title:"Feature Count", field:"Silviculture_FeatureCount", formatter:"money", formatterParams:{
                        precision:0
                    }, topCalc: "sum", topCalcParams:{
                        precision:0,
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:0
                    }},
                    {title:interior_or_nearest, field:"Silviculture_Locations", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Status", field:"STATUS"},
                            {title:"Sale Name", field:"SALE_NAME"},
                            {title:"Total Value", field:"TOTAL_VALUE", formatter:"money", formatterParams:{
                                symbol:"$",
                                precision:0
                            }},
                            {title:"Miles from Fire Edge", field:"dist_mi", formatter:"money", formatterParams:{precision:2}},
                            {title:"Direction", field:"dir"},
                            {title:"Latitude", field:"lat"},
                            {title:"Longitude", field:"lng"}
                        ]
                    }, headerSort:false},
                    {title:"Total Acres", field:"Silviculture_TotalAcres", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }},
                    {title:"Acres by Activity Status", field:"Silviculture_Status_AcreSum", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Activity Status", field:"key head"},
                            {title:"Acres", field: "value head", formatter:"money", formatterParams:{precision:1}}
                        ]
                    }, headerSort:false},
                    {title:"Estimated Value", field:"Silviculture_TotalValue_AttrSum", topCalc: "sum", topCalcParams:{
                        precision:0,
                    }, formatter:"money", formatterParams:{
                        symbol:"$",
                        precision:0
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        symbol:"$",
                        precision:0
                    }}
                ]
            },
            {
                title:"Special Management Considerations",
                headerHozAlign:"center",
                columns:[
                    {title:"Feature Count", field:"SpecMgmt_FeatureCount", formatter:"money", formatterParams:{
                        precision:0
                    }, topCalc: "sum", topCalcParams:{
                        precision:0,
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:0
                    }},
                    {title:interior_or_nearest, field:"SpecMgmt_Locations", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Management Consideration", field:"ManagementConsiderations"},
                            {title:"Miles from Fire Edge", field:"dist_mi", formatter:"money", formatterParams:{precision:2}},
                            {title:"Direction", field:"dir"},
                            {title:"Latitude", field:"lat"},
                            {title:"Longitude", field:"lng"}
                        ]
                    }, headerSort:false},
                    {title:"Count by Consideration Type", field:"SpecMgmt_Consid_AttrCount", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Consideration", field:"key head"},
                            {title:"Feature Count", field: "value head", formatter:"money", formatterParams:{precision:0}}
                        ]
                    }, headerSort:false}
                ]
            },
            {
                title:"Railroad",
                headerHozAlign:"center",
                columns:[
                    {title:"Total Feet", field:"Railroad_TotalFeet", formatter:"money", formatterParams:{
                        precision:0
                    }, topCalc: "sum", topCalcParams:{
                        precision:0,
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:0
                    }},
                    {title:interior_or_nearest, field:"Railroad_Locations", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Route Description", field:"NET_DESC"},
                            {title:"Miles from Fire Edge", field:"dist_mi", formatter:"money", formatterParams:{precision:2}},
                            {title:"Direction", field:"dir"},
                            {title:"Latitude", field:"lat"},
                            {title:"Longitude", field:"lng"}
                        ]
                    }, headerSort:false},
                    {title:"Feet by Route Description", field:"Railroad_NetDesc_FeetSum", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Route Description", field:"key head"},
                            {title:"Feet", field: "value head", formatter:"money", formatterParams:{precision:0}}
                        ]
                    }, headerSort:false},
                ]
            },
            {
                title:"Wind Turbines",
                headerHozAlign:"center",
                columns:[
                    {title:"Feature Count", field:"WindTurb_FeatureCount", formatter:"money", formatterParams:{
                        precision:0
                    }, topCalc: "sum", topCalcParams:{
                        precision:0,
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:0
                    }},
                    {title:interior_or_nearest, field:"WindTurb_Locations", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Project Name", field:"p_name"},
                            {title:"Miles from Fire Edge", field:"dist_mi", formatter:"money", formatterParams:{precision:2}},
                            {title:"Direction", field:"dir"},
                            {title:"Latitude", field:"lat"},
                            {title:"Longitude", field:"lng"}
                        ]
                    }, headerSort:false},
                    {title:"Count by Project Name", field:"WindTurb_ProjName_AttrCount", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Project Name", field:"key head"},
                            {title:"Feature Count", field: "value head", formatter:"money", formatterParams:{precision:0}}
                        ]
                    }, headerSort:false},
                ]
            },
        ]
    });

    // previously called on renderComplete to fix edge case where user applies irrevocable filter combination yielding no results
    // using list_autocomplete_multi_header_filter(), this no longer seems necessary (user can backspace to remove filter)
    // retaining function for now, not currently in use. 20250415.
    const if_table_empty_clear_filters = () => {
        const rows = table.getRows("visible");
        if (rows.length < 1) {
            table.clearHeaderFilter();
        }
    }

    // called on renderComplete
    // assigns alternating color scheme to visible columns
    const color_columns = () => {

        // this variable will alternate between two color assignment values
        let next_color = 'rgb(50, 185, 200, 0.075)';

        // to track which parent column elements have already had a color assigned during this execution (columns loop will pass over multiple of their children)
        const parents_colored = new Set();

        // array of Tabulator JS column components
        const columns = table.getColumns();

        // to determine when a parent column should be skipped (i.e. they have no visible children)
        // using field names because child columns are accessed from the parent using .getDefinition().columns (so we need string identifiers)
        const visible_fields = columns
            .filter(col => col.isVisible())
            .map(col => col.getField());

        for (let column of columns) {

            const parent = column.getParentColumn();

            // skip columns that have no parent and are not visible
            if (!parent && !column.isVisible()) {
                continue;

            // color columns that have no parent and are visible
            } else if (!parent && column.isVisible()) {

                // skip leading frozen columns so we don't make them transparent (these don't require alternating color scheme anyways)
                if (["AkFireNumber", "wfigs_IncidentName", "SpatialInfoType", "VarAppURL"].includes(column.getField())) {
                    continue;
                }
                
                // apply color
                const el = column.getElement();
                el.style.background = next_color;
                const cells = column.getCells();
                for (let cell of cells) {
                    const cell_el = cell.getElement();
                    if (cell.getValue() === "!error!" || cell.getValue() === -1) {
                        cell_el.style.backgroundColor = "rgba(255, 75, 75, 0.25)";
                    } else {
                        cell_el.style.backgroundColor = next_color;
                    }
                }
                // alternate colors - whatever column or group of columns is colored next needs to look different
                next_color = (next_color === 'rgb(255, 182, 18, 0.075)') ? 'rgb(50, 185, 200, 0.075)' : 'rgb(255, 182, 18, 0.075)';

            // color parent columns and their children, based on visibility
            } else if (parent) {

                const parent_title = parent.getDefinition().title;

                const child_fields = parent.getDefinition().columns.map(child => child.field);

                // skip parent columns that have no visible child columns
                const a_child_is_shown = child_fields.some(field => visible_fields.includes(field));
                if (!a_child_is_shown) {
                    continue;
                }

                // ensure parent has not yet been colored during this execution
                if (!parents_colored.has(parent_title)) {

                    // assign color and track parent column title so we don't try to color it and its children again
                    const parent_el = parent.getElement();
                    parent_el.style.background = next_color;
                    parents_colored.add(parent_title);

                    // iterate over all child columns
                    for (let field of child_fields) {

                        const child_column = columns.find(col => col.getField() === field);

                        // apply color
                        const child_el = child_column.getElement();
                        child_el.style.backgroundColor = next_color;
                        const cells = child_column.getCells();
                        for (let cell of cells) {
                            const cell_el = cell.getElement();
                            if (cell.getValue() === "!error!" || cell.getValue() === -1) {
                                cell_el.style.backgroundColor = "rgba(255, 75, 75, 0.25)";
                            } else {
                                cell_el.style.backgroundColor = next_color;
                            }
                        }
                    }
                    // alternate colors - whatever column or group of columns is colored next needs to look different
                    next_color = (next_color === 'rgb(255, 182, 18, 0.075)') ? 'rgb(50, 185, 200, 0.075)' : 'rgb(255, 182, 18, 0.075)';
                }
            }

        }
    }

    // called on tableBuilt
    // hides columns that would otherwise display and be completely empty
    const initial_columns_visibility = () => {

        // initialize sets that will determine which columns to hide and which columns to show
        const columns = table.getColumns();
        let hide_fields = new Set(columns.map(col => col.getField()));
        let show_fields = new Set();

        // iterate over all rows and cells to identify which fields have data
        const rows = table.getRows("all");
        for (let row of rows) {
            const cells = row.getCells();
            for (let cell of cells) {
                if (cell.getValue() != null && cell.getValue() != undefined) {
                    const field_with_data = cell.getColumn().getField();
                    hide_fields.delete(field_with_data);
                    show_fields.add(field_with_data);
                }
            }
        }

        // to prevent certain group columns from collapsing so far that the headers are not legible
        const never_hide = new Set ([
            "MgmtOption_AcreSum.Critical",
            "MgmtOption_AcreSum.Full",
            "MgmtOption_AcreSum.Limited",
            "Jurisd_Owner_AcreSum.Private",
            "Jurisd_Owner_AcreSum.State",
        ]);
        show_fields = new Set([...show_fields, ...never_hide]);

        // hide columns that have NO data, show columns that have ANY data
        for (let field of hide_fields) {
            if (!never_hide.has(field)) {
                table.hideColumn(field);
            }
        }
        for (let field of show_fields) {
            table.showColumn(field);
            const column = table.getColumn(field);
            column.setWidth(true);
        }
    }

    table.on("tableBuilt", initial_columns_visibility);
    table.on("renderComplete", color_columns);
    table.on("columnVisibilityChanged", color_columns);

    table.on("cellClick", function(e, cell) {
        setTimeout(() => {cell.getRow().normalizeHeight()}, 500);
        setTimeout(() => {cell.getColumn().setWidth(true)}, 500);
    });    

}
//
// END MAIN TABULATOR

const load_json_data = async () => {
    const [perims_locs, buf_1, buf_3, buf_5, timestamp] = await Promise.all([
        fetch("./input_json/akdof_perims_locs.json").then(res => res.json()),
        fetch("./input_json/buf_1.json").then(res => res.json()),
        fetch("./input_json/buf_3.json").then(res => res.json()),
        fetch("./input_json/buf_5.json").then(res => res.json()),
        fetch("./input_json/timestamp.json").then(res => res.json())
    ]);
    return { perims_locs, buf_1, buf_3, buf_5, timestamp };
}

const main = async () => {

    const data = await load_json_data();

    document.getElementById("updated-datetime").innerHTML = "Last Updated " + new Date(data.timestamp.datetime).toLocaleString('en-US', {
        hour12: false,
        year: 'numeric',
        month: 'numeric',
        day: 'numeric',
        hour: '2-digit', 
        minute: '2-digit',
    });

    // holds arrays formatted as [ { div id }, { tabulator tag }, { array of json rows } ]
    const builder = [
        ["perims-locs", "#perimeters-and-locations", data.perims_locs],
        ["buf-1", "#one-mile-buffers", data.buf_1],
        ["buf-3", "#three-mile-buffers", data.buf_3],
        ["buf-5", "#five-mile-buffers", data.buf_5]
    ]

    for (let build of builder) {
        const element = document.getElementById(build[0]);
        if (element) {
            build_table(build[1], build[2])
        }
    }
}

main()