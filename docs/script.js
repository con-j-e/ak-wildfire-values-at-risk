// MUTATORS (https://tabulator.info/docs/6.3/mutators#mutators)
//
const utc_timestamp_to_akt_obj = (epoch_milliseconds) => {
    if (epoch_milliseconds === null || epoch_milliseconds === undefined) {
        return epoch_milliseconds;
    }
    const datetime = luxon.DateTime.fromMillis(epoch_milliseconds, {zone:"America/Anchorage"});
    return datetime;
};

const object_keys = (obj) => {
    if (obj === null || obj === undefined) {
        return obj;
    }
    return Object.keys(obj);
};
//
// END MUTATORS

// FORMATTERS (https://tabulator.info/docs/6.3/format#format-custom)
//
const html_link = (cell, formatterParams) => {
    const url = cell.getValue();
    if (url === null || url === undefined) {
        return url;
    }
    const label = formatterParams.label;
    return '<a href="' + url + '" target="_blank">' + label + '</a>';
};

// not yet implemented
// similar approach for weather station links may work
const air_nav_links = (obj) => {
    if (obj === null || obj === undefined) {
        return null;
    }
    const loc_ids = Object.keys(obj);
    const links = loc_ids.map(loc_id => `<a href="https://www.airnav.com/airport/${loc_id}" target="_blank">${loc_id}</a>`);
    return links.join(', ');
};

const nested_tabulator = (cell, formatterParams, onRendered) => {

    const rows = cell.getValue()
    if (rows === null || rows === undefined) {
        return rows;
    }

    // configure elements
    var holderEl = document.createElement("div");
    var tableEl = document.createElement("div");
    holderEl.style.boxSizing = "border-box";
    holderEl.style.padding = "10px 30px 10px 10px";
    holderEl.style.borderTop = "1px solid #333";
    holderEl.style.borderBotom = "1px solid #333";
    holderEl.style.background = "#ddd";
    tableEl.style.border = "1px solid #333";
    holderEl.appendChild(tableEl);

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
//
// END HEADER FILTER FUNCTIONS


// MAIN TABULATOR
//
const build_table = (tag, rows_array) => {
    const table = new Tabulator(tag, {
        // persistence has pros and cons. look into options for resetting table state.
        /*
        persistence:{ 
            sort: true, 
            headerFilter: true, 
            page: true, 
            columns: true, 
        },
        persistenceID: `pid_${tag.slice(1)}`,
        */
        height:"100%",  
        layout: "fitData",
        pagination: "local",
        paginationSize: 10, 
        paginationCounter: "rows",
        movableColumns: true, 
        resizableRows: true,
        data: rows_array,
        headerFilterLiveFilterDelay:900,
        columns: [
            {title:"Fire Number", field:"AkFireNumber", frozen:true, headerFilter:"list", headerFilterParams:{
                valuesLookup:"active",
                sort:'desc',
                autocomplete:true,
                allowEmpty:true,
                listOnEmpty:true
            }, headerFilterFunc:list_autocomplete_multi_header_filter},
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

            // TODO use nearest features analysis info here
            // 
            {title:"Community Count", field:"Community_FeatureCount", formatter:"money", formatterParams:{
                precision:0
            }, topCalc: "sum", topCalcParams:{
                precision:0,
            }},
            {title:"Community Names", field:"Community_Name_AttrCount", mutator:object_keys, formatter:"array", headerFilter:"input"},
            
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
                    {title:"State", field:"Jurisd_Owner_AcreSum.State", formatter:"money", formatterParams:{
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
                    {title:"BLM", field:"Jurisd_Owner_AcreSum.BLM", formatter:"money", formatterParams:{
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
                    {title:"ANCSA", field:"Jurisd_Owner_AcreSum.ANCSA", formatter:"money", formatterParams:{
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
                    {title:"USFWS", field:"Jurisd_Owner_AcreSum.USFWS", formatter:"money", formatterParams:{
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
                    {title:"OthLoc", field:"Jurisd_Owner_AcreSum.OthLoc", formatter:"money", formatterParams:{
                        precision:1
                    }, topCalc: "sum", topCalcParams:{
                        precision:1
                    }, topCalcFormatter:"money", topCalcFormatterParams:{
                        precision:1
                    }}
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
                    {title:"Feet by Conduction Type", field:"AkPowerLine_CondType_FeetSum", variableHeight:true, formatter:nested_tabulator, formatterParams:{
                        layout: "fitDataStretch",
                        pagination: "local",
                        paginationSize: 5, 
                        paginationCounter: "rows",
                        movableColumns: true, 
                        resizableRows: true,
                        columns:[
                            {title:"Conduction Type", field:"key head"},
                            {title:"Feet", field: "value head", formatter:"money", formatterParams:{precision:1}}
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
                    },    
                        headerSort:false},
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
                    },    
                        headerSort:false},
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
                    },    
                        headerSort:false},
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

            //TODO use nearest feature analysis info
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
                    {title:"Station Names", field:"WeatherStation_Name_AttrCount", mutator:object_keys, formatter:"array"}
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
                                const alias = (value == "0") ? "Non-active" : "Active";
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
        ]
    });

    const format_columns = () => {

        // load all column components
        const columns = table.getColumns();

        // initialize sets that will determine which columns to hide and which columns to show
        let hide_fields = new Set(columns.map(col => col.getField()));
        let show_fields = new Set();

        // load all visible row components
        const rows = table.getRows("visible");

        // heuristic, efficacy not yet proven
        // assume that if there are no visible rows, the user has broken something with an impossible and irrevocable header filter
        if (rows.length < 1) {
            table.clearHeaderFilter();
        }

        // iterate over all rows and cells to identify which fields have data
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

        //^ this block is almost a functional option for handling group column collapse in a more dynamic way
        //^ not yet predictable enough for implementation but may be worth future exploration
        //^ in the meantime, use of a never_hide set (see below) is more stable
        /*
        // iterate over all columns that will be visible and ensure proper columns widths
        //for (let field of show_fields) {
        for (let column of columns) {
            if (!show_fields.has(column.getField())) {
                continue
            }
            //const column = table.getColumn(field);
            const parent = column.getParentColumn();
            if (parent !== false) {
                const column_width = column.getWidth();
                const parent_width = parent.getWidth();
                const wider_parent_px = parent_width * 3;
                // this heuristic may need to be adjusted, but its working nicely as of 20250329. max parent width determined by management option acres.
                if (column_width >= parent_width && wider_parent_px < 350) {
                    parent.setWidth(wider_parent_px);
                    column.setWidth(wider_parent_px);
                } else {
                    column.setWidth(true);
                    column.setWidth(column.getWidth() + 25);
                }
    
            } else {
                column.setWidth(true);
            }
        }
        */

        // to prevent certain group columns from collapsing so far that the headers are not legible
        // additions may be necessary if other problematic group columns are identified
        // may be worth opening a gh issue to address this behavior
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

        // initialize sets to track which elements have already had a color assigned during this execution
        // this is essential to preserve the desired alternating color scheme
        const columns_colored = new Set();
        const parents_colored = new Set();

        // initialize first color to assign
        // this variable will alternate between two color assignment values
        let next_color = 'rgb(50, 185, 200, 0.075)';

        // iterate over all columns
        // using the columns object (instead of show_fields, for example) is essential for following the order in which columns display left-to-right
        for (let column of columns) {

            // skip columns that have no parent and are hidden
            const parent = column.getParentColumn();
            if (parent === false && hide_fields.has(column.getField()) ) {
                continue;
            }

            // color parent columns and their children
            if (parent !== false) {
                const children = parent.getDefinition().columns;
                const fields = children.map(child => child.field);

                // skip parent columns that have no child columns which will be shown
                const a_child_is_shown = fields.some(item => show_fields.has(item));
                if (!a_child_is_shown) {
                    continue
                }

                // ensure parent has not yet been colored during this execution
                if (!parents_colored.has(parent.getDefinition().title)) {

                    // assign color and track parent column title so we don't try to color it again
                    const parent_el = parent.getElement();
                    parent_el.style.background = next_color;
                    parents_colored.add(parent.getDefinition().title);

                    // iterate over all child columns
                    for (let field of fields) {
                        const child_column = table.getColumn(field);

                        // ensure child column has not yet been colored during this execution
                        if (!columns_colored.has(child_column.getField())) {

                            // assign color and track child column field so we don't try to color it again
                            const child_el = child_column.getElement();
                            child_el.style.backgroundColor = next_color;
                            columns_colored.add(child_column.getField());

                            // iterate over all cells in the child column and color them
                            // there is no circumstance in which an execution should color a child column but not its cells
                            const cells = child_column.getCells();
                            for (let cell of cells) {
                                const cell_el = cell.getElement();
                                cell_el.style.backgroundColor = next_color;
                            }
                        }
                    }
                    // alternate colors - whatever column or group of columns is colored next needs to look different
                    next_color = (next_color === 'rgb(255, 182, 18, 0.075)') ? 'rgb(50, 185, 200, 0.075)' : 'rgb(255, 182, 18, 0.075)';
                }
            }

            // skip leading frozen columns so we don't make them transparent (these don't require alternating color scheme anyways)
            // skip any columns which were colored above with their parent
            if (["AkFireNumber", "wfigs_IncidentName", "SpatialInfoType", "VarAppURL"].includes(column.getField()) || columns_colored.has(column.getField())) {
                continue;
            }
            
            // color any column which has not been skipped by preceeding logic
            const el = column.getElement();
            el.style.background = next_color;
            const cells = column.getCells();
            for (let cell of cells) {
                const cell_el = cell.getElement();
                cell_el.style.backgroundColor = next_color;
            }
            // alternate colors - whatever column or group of columns is colored next needs to look different
            next_color = (next_color === 'rgb(255, 182, 18, 0.075)') ? 'rgb(50, 185, 200, 0.075)' : 'rgb(255, 182, 18, 0.075)';
        }
    }

    table.on("renderComplete", format_columns);

    table.on("cellClick", function(e, cell) {
        setTimeout(() => {cell.getRow().normalizeHeight()}, 500)
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

