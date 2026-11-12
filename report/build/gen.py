"""Generate the Hale Joli Solar 2.0 PBIR report definition."""
import json, os, hashlib, shutil, sys
S = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(S, 'report_new'); CUR = os.path.join(S, 'report_cur')
VC = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json"
PG = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.0.0/schema.json"
ST, BU, D, T = "Solar Telemetry", "Battery Units", "Dates", "Time"

# ---- palette: entity-fixed, validated on the dark surface -------------------
SOLAR, HOME, GRID_IN, GRID_OUT, BATT = "#EDA100", "#2A78D6", "#E34948", "#6250D6", "#1BAF7A"
BATT_LIGHT, NEUTRAL, ORANGE = "#9ADBC3", "#9A9890", "#EB6834"
PAGE_BG, CARD_BG, CARD_BORDER, INK, INK2, INK3 = "#F5F3EE", "#FFFFFF", "#E3DFD5", "#0B0B0B", "#52514E", "#7A7870"
AMBER_TEXT, HOVER_BG = "#9A6400", "#EDE9DF"   # text-safe amber for the page title; hover fill

def hid(*parts): return hashlib.sha1("|".join(parts).encode()).hexdigest()[:20]
def lit(v):
    if isinstance(v, bool): s = "true" if v else "false"
    elif isinstance(v, int): s = f"{v}L"
    elif isinstance(v, float): s = f"{v}D"
    else: s = "'" + str(v).replace("'", "''") + "'"
    return {"expr": {"Literal": {"Value": s}}}
def col(hexv): return {"solid": {"color": lit(hexv)}}
def props(selector=None, **kw):
    o = {"properties": {k: (v if isinstance(v, dict) else lit(v)) for k, v in kw.items()}}
    if selector is not None: o["selector"] = selector
    return o
def src(t): return {"SourceRef": {"Entity": t}}
def mexpr(t, n): return {"Measure": {"Expression": src(t), "Property": n}}
def cexpr(t, n): return {"Column": {"Expression": src(t), "Property": n}}
def M(n, display=None, t=ST, fmt=None):
    p = {"field": mexpr(t, n), "queryRef": f"{t}.{n}", "nativeQueryRef": display or n}
    if display: p["displayName"] = display
    if fmt: p["format"] = fmt
    return p
def C(t, n, display=None, fmt=None):
    p = {"field": cexpr(t, n), "queryRef": f"{t}.{n}", "nativeQueryRef": display or n, "active": True}
    if display: p["displayName"] = display
    if fmt: p["format"] = fmt
    return p
def plain(p): return {k: v for k, v in p.items() if k != "active"}
def qref(p): return p["queryRef"]
def series_color(p, hexv): return props({"metadata": qref(p)}, fill=col(hexv))
def value_color(t, n, value, hexv):
    return props({"data": [{"scopeId": {"Comparison": {"ComparisonKind": 0, "Left": cexpr(t, n), "Right": {"Literal": {"Value": f"'{value}'"}}}}}]}, fill=col(hexv))
def sort(p, direction="Ascending"): return {"sort": [{"field": p["field"], "direction": direction}]}

class Page:
    def __init__(self, key, title, blurb):
        self.name = hid("page", key); self.title = title; self.blurb = blurb; self.visuals = []; self.z = 0
    def add(self, key, vtype, x, y, w, h, roles=None, objects=None, vco=None, title=None, subtitle=None, sortdef=None, extra=None, filters=None):
        self.z += 1000
        v = {"visualType": vtype}
        if roles:
            v["query"] = {"queryState": {r: {"projections": ps} for r, ps in roles.items()}}
            if sortdef: v["query"]["sortDefinition"] = sortdef
        if objects: v["objects"] = objects
        vco = dict(vco or {})
        if title is not None: vco["title"] = [props(show=True, text=title)]
        if subtitle is not None: vco["subTitle"] = [props(show=True, text=subtitle)]
        if vco: v["visualContainerObjects"] = vco
        if extra: v.update(extra)
        v["drillFilterOtherVisuals"] = True
        c = {"$schema": VC, "name": hid(self.name, key), "position": {"x": x, "y": y, "z": self.z, "height": h, "width": w, "tabOrder": self.z}, "visual": v}
        if filters: c["filterConfig"] = {"filters": filters}
        self.visuals.append(c); return c

NAV = [("overview", "Overview"), ("rhythm", "Daily Rhythm"), ("battery", "Battery"), ("money", "Grid & Money"), ("seasons", "Seasons"), ("health", "Power & Health")]
BARE = {"background": [props(show=False)], "border": [props(show=False)], "dropShadow": [props(show=False)]}
def chrome(pg):
    """Header shared by every page: wordmark + page title, page navigator, synced slicers."""
    pg.add("title", "textbox", 16, 2, 304, 70, objects={"general": [{"properties": {"paragraphs": [
        {"textRuns": [{"value": "Hale Joli", "textStyle": {"fontWeight": "bold", "fontFamily": "Segoe UI", "fontSize": "16pt", "color": INK}},
                      {"value": "  /  " + pg.title, "textStyle": {"fontFamily": "Segoe UI", "fontSize": "16pt", "color": AMBER_TEXT}}]},
        {"textRuns": [{"value": pg.blurb, "textStyle": {"fontFamily": "Segoe UI", "fontSize": "9pt", "color": INK3}}]}]}}]}, vco=BARE)
    bw, gap, x0 = 102, 7, 324
    for n, (key, label) in enumerate(NAV):
        sel = (hid("page", key) == pg.name)
        pg.add("nav_" + key, "actionButton", x0 + n * (bw + gap), 18, bw, 36, objects={
            "icon": [props(show=False), props({"id": "default"}, shapeType="blank")],
            "text": [props(show=True), props({"id": "default"}, text=label, fontColor=col(INK if sel else INK2), fontSize=9,
                                             fontFamily="Segoe UI Semibold" if sel else "Segoe UI", horizontalAlignment="center"),
                     props({"id": "hover"}, text=label, fontColor=col(INK))],
            "fill": [props(show=True), props({"id": "default"}, fillColor=col(SOLAR if sel else CARD_BG), transparency=0),
                     props({"id": "hover"}, fillColor=col(SOLAR if sel else HOVER_BG), transparency=0)],
            "outline": [props(show=not sel), props({"id": "default"}, lineColor=col(CARD_BORDER), weight=1)],
            "shape": [props({"id": "default"}, tileShape="rectangleRounded", rectangleRoundedCurve=40)]},
            vco=dict(BARE, padding=[props(top=0, bottom=0, left=0, right=0)],
                     visualLink=[props(show=True, type="PageNavigation", navigationSection=hid("page", key))]))
    for i, (f, grp) in enumerate([("Year", "Year"), ("Month", "Month")]):
        pg.add("slicer" + f, "slicer", 980 + i * 144, 6, 136, 60, roles={"Values": [C(D, f)]},
               objects={"data": [props(mode="Dropdown")], "header": [props(show=True, textSize=8)], "items": [props(textSize=9)],
                        "selection": [props(selectAllCheckboxEnabled=True)]},
               vco={"padding": [props(top=2, bottom=2, left=6, right=6)]},
               extra={"syncGroup": {"groupName": grp, "fieldChanges": True, "filterChanges": True}})

def kpis(pg, items, colors):
    ps = [M(n, d, t) if isinstance(n, str) else n for (n, d, t) in items]
    objs = {"accentBar": [props({"metadata": qref(p)}, show=True, color=col(c), position="Left", width=4) for p, c in zip(ps, colors)],
            "value": [props({"id": "default"}, fontSize=20, horizontalAlignment="left")],
            "label": [props({"id": "default"}, fontSize=9, horizontalAlignment="left")]}
    pg.add("kpis", "cardVisual", 12, 72, 1256, 108, roles={"Data": ps}, objects=objs, vco=dict(BARE, padding=[props(top=0, bottom=0, left=0, right=0)]))

AX = {"categoryAxis": [props(showAxisTitle=False)], "valueAxis": [props(showAxisTitle=False)]}
def chart(pg, key, vtype, x, y, w, h, cat, ys, colors, title, subtitle=None, series=None, sortcat=False, legend=None, more=None, filters=None, extra_dp=None):
    roles = {"Category": [cat], "Y": ys}
    if series: roles["Series"] = [series]
    objs = dict(AX); objs["dataPoint"] = [series_color(p, c) for p, c in zip(ys, colors)] + (extra_dp or [])
    if len(ys) == 1 and not series: objs["dataPoint"].insert(0, props(defaultColor=col(colors[0])))
    show_legend = legend if legend is not None else (len(ys) > 1 or series is not None)
    objs["legend"] = [props(show=show_legend, position="Top", showTitle=False)]
    if more: objs.update(more)
    return pg.add(key, vtype, x, y, w, h, roles=roles, objects=objs, title=title, subtitle=subtitle, sortdef=sort(cat) if sortcat else None, filters=filters)

def heat(measure_t, measure_n, maxhex):
    return {"solid": {"color": {"expr": {"FillRule": {"Input": mexpr(measure_t, measure_n), "FillRule": {"linearGradient2": {
        "min": {"color": {"Literal": {"Value": f"'{CARD_BG}'"}}}, "max": {"color": {"Literal": {"Value": f"'{maxhex}'"}}},
        "nullColoringStrategy": {"strategy": {"Literal": {"Value": "'asZero'"}}}}}}}}}}
def matrix(pg, key, x, y, w, h, row, colf, val, maxhex, title, subtitle):
    objs = {"subTotals": [props(rowSubtotals=False, columnSubtotals=False)],
            "values": [props(fontSize=7), {"properties": {"backColor": heat(ST, val["field"]["Measure"]["Property"], maxhex)},
                        "selector": {"data": [{"dataViewWildcard": {"matchingOption": 1}}], "metadata": qref(val)}}],
            "columnHeaders": [props(fontSize=7, autoSizeColumnWidth=True)], "rowHeaders": [props(fontSize=7)],
            "grid": [props(gridVertical=False, gridHorizontal=False, rowPadding=0, textSize=7)]}
    pg.add(key, "pivotTable", x, y, w, h, roles={"Rows": [row], "Columns": [colf], "Values": [val]}, objects=objs, title=title, subtitle=subtitle,
           vco={"stylePreset": [props(name="Minimal")]})
def table(pg, key, x, y, w, h, cols, title, subtitle=None, sortdef=None, filters=None):
    pg.add(key, "tableEx", x, y, w, h, roles={"Values": [plain(c) for c in cols]}, objects={"values": [props(fontSize=9)], "columnHeaders": [props(fontSize=9)],
           "total": [props(totals=False)], "grid": [props(gridVertical=False, rowPadding=3)]}, title=title, subtitle=subtitle, sortdef=sortdef,
           vco={"stylePreset": [props(name="Minimal")]}, filters=filters)

R1Y, R1H, R2Y, R2H = 184, 260, 456, 248
pages = []

# ---------------------------------------------------------------- 1 Overview
p = Page("overview", "Overview", "Solar, home, battery and grid in Kona"); chrome(p)
kpis(p, [("Production kWh", "Solar produced (kWh)", ST), ("Consumption kWh", "Home used (kWh)", ST), ("Self-Sufficiency Rate", "Self-powered", ST),
         ("Battery Discharge kWh", "From the battery (kWh)", ST), ("Grid Import kWh", "From the grid (kWh)", ST), ("Solar Savings", "Solar savings", ST)],
     [SOLAR, HOME, NEUTRAL, BATT, GRID_IN, SOLAR])
chart(p, "daily", "lineChart", 16, R1Y, 800, R1H, C(D, "Date"), [M("Production kWh", "Solar produced"), M("Consumption kWh", "Home used")], [SOLAR, HOME],
      "Solar produced vs home used", "kWh per day")
sd, bd, gi = M("Solar Direct to Home kWh", "Solar, direct"), M("Battery Discharge kWh", "Battery"), M("Grid Import kWh", "Grid")
p.add("mix", "donutChart", 828, R1Y, 436, R1H, roles={"Y": [sd, bd, gi]},
      objects={"dataPoint": [series_color(sd, SOLAR), series_color(bd, BATT), series_color(gi, GRID_IN)], "legend": [props(show=True, position="Bottom")],
               "labels": [props(show=True, labelStyle="Percent of total", percentageDecimalPlaces=0)], "slices": [props(innerRadiusRatio=68)]},
      title="Where the home's energy came from", subtitle="Share of kWh consumed")
chart(p, "monthmix", "columnChart", 16, R2Y, 620, R2H, C(D, "Year-Month"), [sd, bd, gi], [SOLAR, BATT, GRID_IN],
      "Monthly supply mix", "kWh consumed, by source", sortcat=True)
chart(p, "selfsuff", "lineChart", 648, R2Y, 616, R2H, C(D, "Year-Month"), [M("Self-Sufficiency Rate", "Self-powered")], [BATT],
      "Self-powered share by month", "Consumption not drawn from the grid", sortcat=True, more={"labels": [props(show=True)]})
pages.append(p)

# ------------------------------------------------------------ 2 Daily Rhythm
p = Page("rhythm", "Daily Rhythm", "A typical day, hour by hour"); chrome(p)
kpis(p, [("Average Daily Production", "Avg daily solar (kWh)", ST), ("Average Daily Consumption", "Avg daily use (kWh)", ST),
         ("Average Daily Battery Discharge", "Avg daily from battery (kWh)", ST), ("Average Daily Grid Import", "Avg daily from grid (kWh)", ST),
         ("Average Hourly Consumption", "Avg house load (kW)", ST), ("Days in Period", "Days of data", ST)],
     [SOLAR, HOME, BATT, GRID_IN, HOME, NEUTRAL])
chart(p, "profile", "lineChart", 16, R1Y, 620, 236, C(T, "Hour"), [M("Average Hourly Production", "Solar"), M("Average Hourly Consumption", "House load")], [SOLAR, HOME],
      "An average day", "Average kW by hour of day", sortcat=True)
chart(p, "hourmix", "columnChart", 648, R1Y, 616, 236, C(T, "Hour"), [sd, bd, gi], [SOLAR, BATT, GRID_IN],
      "What powers the home, hour by hour", "kWh consumed, by source", sortcat=True)
matrix(p, "heat", 16, 432, 668, 272, C(D, "Year-Month", "Month"), C(T, "Hour"), M("Average Hourly Production", "Avg kW", fmt="0.0"), SOLAR,
       "Solar by hour and month", "Average kW; darker is more")
chart(p, "weekday", "clusteredColumnChart", 696, 432, 568, 272, C(D, "Day Name Short"), [M("Average Daily Consumption", "Avg daily use")], [HOME],
      "Home use by day of week", "Average kWh per day", sortcat=True, more={"labels": [props(show=True)]})
pages.append(p)

# ----------------------------------------------------------------- 3 Battery
p = Page("battery", "Battery", "Charge, backup headroom, unit health"); chrome(p)
kpis(p, [("Latest Battery SOC", "Charge now (%)", ST), ("Usable Backup Energy kWh", "Usable above reserve (kWh)", ST), ("Backup Hours at Recent Load", "Backup hours at last 24h load", ST),
         ("Battery Charge kWh", "Charged (kWh)", ST), ("Battery Discharge kWh", "Discharged (kWh)", ST), ("Unit SoC Spread", "Spread between units (pts)", BU)],
     [BATT, BATT, BATT, BATT_LIGHT, BATT, NEUTRAL])
chart(p, "soc", "lineChart", 16, R1Y, 800, R1H, C(D, "Date"), [M("Avg Battery SOC", "Daily average"), M("Min Battery SOC", "Daily low")],
      [BATT, NEUTRAL], "Battery charge level", "Percent, per day")
chart(p, "chgdis", "clusteredColumnChart", 828, R1Y, 436, R1H, C(D, "Year-Month"), [M("Battery Charge kWh", "Charged"), M("Battery Discharge kWh", "Discharged")],
      [BATT_LIGHT, BATT], "Charged vs discharged", "kWh per month", sortcat=True)
unit_colors = [value_color(BU, "UnitName", f"Battery {i}", c) for i, c in zip(range(1, 5), [HOME, ORANGE, BATT, SOLAR])]
chart(p, "unitsoc", "lineChart", 16, R2Y, 500, R2H, C(BU, "ReadingDateTimeLocal"), [M("Unit State of Charge", "Charge %", BU)], [], "Charge by unit",
      "Percent, every 15 minutes", series=C(BU, "UnitName"), extra_dp=unit_colors)
chart(p, "unittemp", "lineChart", 528, R2Y, 380, R2H, C(BU, "ReadingDateTimeLocal"), [M("Unit Temperature", "Temperature", BU)], [], "Temperature by unit",
      "Degrees C", series=C(BU, "UnitName"), extra_dp=unit_colors)
table(p, "unittable", 920, R2Y, 344, R2H, [C(BU, "UnitName", "Unit"), M("Unit State of Charge", "Avg %", BU), M("Unit Max Temperature", "Max C", BU),
      M("Unit Peak Charge kW", "Peak in kW", BU), M("Unit Peak Discharge kW", "Peak out kW", BU)], "Unit scorecard", sortdef=sort(C(BU, "UnitName")))
pages.append(p)

# ------------------------------------------------------------ 4 Grid & Money
p = Page("money", "Grid & Money", "Grid use, cost, and what solar saved"); chrome(p)
kpis(p, [("Cost Without Solar", "Bill without solar", ST), ("Net Grid Cost", "Net grid cost", ST), ("Solar Savings", "Solar savings", ST),
         ("Average Daily Grid Cost", "Avg daily grid cost", ST), ("Projected Monthly Cost", "Projected monthly cost", ST), ("TOU Savings vs Flat Rate", "Time-of-use plan would save", ST)],
     [NEUTRAL, GRID_IN, SOLAR, GRID_IN, GRID_IN, GRID_OUT])
chart(p, "bill", "clusteredColumnChart", 16, R1Y, 800, R1H, C(D, "Year-Month"), [M("Cost Without Solar", "Without solar"), M("Net Grid Cost", "Actual net cost")],
      [NEUTRAL, GRID_IN], "Monthly bill, with and without solar", "Dollars at the flat rate", sortcat=True)
chart(p, "tod", "clusteredBarChart", 828, R1Y, 436, R1H, C(T, "TimeOfDay"), [M("Grid Import kWh", "From the grid")], [GRID_IN],
      "When the grid gets used", "kWh imported, by time of day", more={"labels": [props(show=True)]})
chart(p, "impexp", "lineChart", 16, R2Y, 620, R2H, C(D, "Date"), [M("Grid Import kWh", "Imported")], [GRID_IN],
      "Grid import", "kWh per day; this system exports almost nothing")
ym = C(D, "Year-Month", "Month")
table(p, "monthtable", 648, R2Y, 616, R2H, [ym, M("Production kWh", "Solar kWh"), M("Consumption kWh", "Used kWh"), M("Grid Import kWh", "Grid kWh"),
      M("Self-Sufficiency Rate", "Self-powered"), M("Net Grid Cost", "Net cost"), M("Solar Savings", "Savings")], "Month by month", sortdef=sort(ym, "Descending"))
pages.append(p)

# ------------------------------------------------------- 5 Seasons & Records
p = Page("seasons", "Seasons", "The year at a glance and record days"); chrome(p)
kpis(p, [("Best Day Production kWh", "Record day (kWh)", ST), ("Best Day", "Record set on", ST), ("Average Daily Production", "Avg daily solar (kWh)", ST),
         ("Average Daily Consumption", "Avg daily use (kWh)", ST), ("Self-Consumption Rate", "Solar used on site", ST), ("Days in Period", "Days of data", ST)],
     [SOLAR, SOLAR, SOLAR, HOME, BATT, NEUTRAL])
matrix(p, "calendar", 16, R1Y, 768, 284, C(D, "Year-Month", "Month"), C(D, "Day of Month"), M("Production kWh", "kWh", fmt="0"), SOLAR,
       "Production calendar", "kWh per day; darker is more")
dt = C(D, "Date", "Day", fmt="d MMM yyyy")
table(p, "bestdays", 796, R1Y, 468, 284, [dt, M("Production kWh", "Solar kWh"), M("Consumption kWh", "Used kWh"), M("Grid Import kWh", "Grid kWh"), M("Self-Sufficiency Rate", "Self-powered")], "Best solar days",
      sortdef={"sort": [{"field": mexpr(ST, "Production kWh"), "direction": "Descending"}]})
y2, h2 = R1Y + 284 + 12, 720 - 16 - (R1Y + 284 + 12)
chart(p, "season", "clusteredColumnChart", 16, y2, 616, h2, C(D, "Season"), [M("Average Daily Production", "Solar"), M("Average Daily Consumption", "Home use")], [SOLAR, HOME],
      "An average day, by season", "kWh per day")
chart(p, "monthprod", "clusteredColumnChart", 644, y2, 620, h2, C(D, "Year-Month"), [M("Production kWh", "Solar produced")], [SOLAR],
      "Solar by month", "kWh", sortcat=True)
pages.append(p)

# --------------------------------------------------------- 6 Power & Health
p = Page("health", "Power & Health", "Live Envoy signals since 6 Nov 2026"); chrome(p)
kpis(p, [("Last Reading Short", "Last reading (HST)", ST), ("Peak Production kW", "Peak solar (kW)", ST), ("Peak House Load kW", "Peak house load (kW)", ST),
         ("Peak Grid Import kW", "Peak grid draw (kW)", ST), ("Panels Producing Peak", "Panels producing (of 40)", ST), ("Off-Grid Hours", "Hours off grid", ST)],
     [NEUTRAL, SOLAR, HOME, GRID_IN, SOLAR, GRID_OUT])
rt = C(ST, "ReadingDateTimeLocal", "Time")
chart(p, "peaks", "lineChart", 16, R1Y, 800, R1H, rt, [M("Peak Production kW", "Solar"), M("Peak House Load kW", "House load"), M("Peak Grid Import kW", "Grid draw")],
      [SOLAR, HOME, GRID_IN], "Peak power", "Highest kW in each 15 minutes")
ha_only = [{"name": hid("f", "status"), "field": cexpr(ST, "Source"), "type": "Categorical", "filter": {"Version": 2, "From": [{"Name": "s", "Entity": ST, "Type": 0}],
            "Where": [{"Condition": {"In": {"Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": "Source"}}],
                                             "Values": [[{"Literal": {"Value": "'home_assistant'"}}]]}}}]}}]
chart(p, "status", "clusteredBarChart", 828, R1Y, 436, R1H, C(ST, "BatteryStatus", "Battery state"), [M("Interval Hours", "Hours")], [BATT],
      "Hours by battery state", "Charging, discharging, idle or full", more={"labels": [props(show=True)]}, filters=ha_only)
chart(p, "stored", "areaChart", 16, R2Y, 620, R2H, rt, [M("Battery Stored kWh", "Stored"), M("Battery Reserve kWh", "Backup reserve")], [BATT, NEUTRAL],
      "Stored energy vs backup reserve", "kWh")
chart(p, "panels", "lineChart", 648, R2Y, 300, R2H, rt, [M("Panels Producing Peak", "Panels")], [SOLAR], "Panels producing", "Of 40 installed")
table(p, "sources", 960, R2Y, 304, R2H, [C(ST, "Source"), M("Interval Hours", "Hours"), M("Last Reading", "Latest")], "Where the data comes from")
pages.append(p)

# ---- theme --------------------------------------------------------------------
theme = {"name": "Hale Joli Day", "dataColors": [SOLAR, HOME, GRID_IN, GRID_OUT, BATT, ORANGE, "#D55181", "#008300"],
    "foreground": INK, "foregroundNeutralSecondary": INK2, "foregroundNeutralTertiary": INK3, "background": CARD_BG, "backgroundLight": "#F8F6F1",
    "backgroundNeutral": CARD_BORDER, "tableAccent": SOLAR, "good": "#0CA30C", "neutral": "#FAB219", "bad": "#D03B3B",
    "maximum": SOLAR, "center": "#F6D080", "minimum": CARD_BG, "hyperlink": "#256ABF",
    "textClasses": {"callout": {"fontSize": 24, "fontFace": "Segoe UI Semibold", "color": INK}, "title": {"fontSize": 12, "fontFace": "Segoe UI Semibold", "color": INK},
                    "header": {"fontSize": 10, "fontFace": "Segoe UI Semibold", "color": INK}, "label": {"fontSize": 9, "fontFace": "Segoe UI", "color": INK2}},
    "visualStyles": {"*": {"*": {
        "background": [{"show": True, "color": {"solid": {"color": CARD_BG}}, "transparency": 0}],
        "border": [{"show": True, "color": {"solid": {"color": CARD_BORDER}}, "radius": 10, "width": 1}],
        "dropShadow": [{"show": False}],
        "title": [{"show": True, "fontColor": {"solid": {"color": INK}}, "fontSize": 12, "fontFamily": "Segoe UI Semibold"}],
        "subTitle": [{"show": True, "fontColor": {"solid": {"color": INK3}}, "fontSize": 9, "fontFamily": "Segoe UI"}],
        "padding": [{"top": 8, "bottom": 8, "left": 12, "right": 12}],
        "lineStyles": [{"strokeWidth": 2}]}},
      "page": {"*": {"background": [{"color": {"solid": {"color": PAGE_BG}}, "transparency": 0}], "outspace": [{"color": {"solid": {"color": PAGE_BG}}, "transparency": 0}]}}}}

# ---- write --------------------------------------------------------------------
def write(path, obj):
    fp = os.path.join(OUT, path); os.makedirs(os.path.dirname(fp), exist_ok=True)
    with open(fp, 'w') as f: json.dump(obj, f, indent=2)
if os.path.exists(OUT): shutil.rmtree(OUT)
for f in ["definition.pbir", "definition/version.json", "StaticResources/SharedResources/BaseThemes/CY25SU12.json"]:
    os.makedirs(os.path.dirname(os.path.join(OUT, f)), exist_ok=True); shutil.copy(os.path.join(CUR, f), os.path.join(OUT, f))
rep = json.load(open(os.path.join(CUR, "definition/report.json")))
TH = "HaleJoliDay.json"
rep["themeCollection"]["customTheme"] = {"name": TH, "reportVersionAtImport": rep["themeCollection"]["baseTheme"]["reportVersionAtImport"], "type": "RegisteredResources"}
rep["resourcePackages"].append({"name": "RegisteredResources", "type": "RegisteredResources", "items": [{"name": TH, "path": TH, "type": "CustomTheme"}]})
write("definition/report.json", rep); write("StaticResources/RegisteredResources/" + TH, theme)
write("definition/pages/pages.json", {"$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.0.0/schema.json",
      "pageOrder": [p.name for p in pages], "activePageName": pages[0].name})
for p in pages:
    write(f"definition/pages/{p.name}/page.json", {"$schema": PG, "name": p.name, "displayName": p.title, "displayOption": "FitToPage", "height": 720, "width": 1280,
          "objects": {"background": [props(color=col(PAGE_BG), transparency=0)], "outspace": [props(color=col(PAGE_BG), transparency=0)]}})
    for v in p.visuals: write(f"definition/pages/{p.name}/visuals/{v['name']}/visual.json", v)
print("pages", len(pages), "| visuals", sum(len(p.visuals) for p in pages))
for p in pages: print(f"  {p.title:<20} {len(p.visuals)} visuals  {p.name}")
