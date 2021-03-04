from flask import Flask, render_template, request, json

from simporter.constants.api_constants import (
    START_DATE,
    END_DATE,
    TYPE,
    GROUPING,
    EXAMPLE_URL, DATE_VIEW, GROUPING_WEEKLY, GROUPING_MONTHLY,
    GROUPING_BI_WEEKLY, TYPE_CUMULATIVE, TYPE_USUAL)

from simporter.constants.data_constants import (
    ASIN,
    BRAND,
    ID,
    SOURCE,
    STARS,
    TIMELINE,
    DATA_PATH,
    TIMESTAMP)

from simporter.utils.simporter_data_util import SimporterDataUtil

app = Flask(__name__)
simporter_data = SimporterDataUtil.load_data_csv(path=DATA_PATH)


@app.route("/", methods=["GET"])
def home_page():
    return render_template(template_name_or_list=["index.html"])


@app.route("/api/info", methods=["GET"])
def api_info():
    attrs = SimporterDataUtil.data_attrs(data=simporter_data)
    attrs_values = {}
    additional_options = {GROUPING: [GROUPING_WEEKLY,
                                     GROUPING_MONTHLY,
                                     GROUPING_BI_WEEKLY],
                          TYPE: [TYPE_CUMULATIVE,
                                 TYPE_USUAL]}

    for attr in attrs:
        values = simporter_data[attr].unique()

        values.sort()
        if attr == TIMESTAMP:
            attr = DATE_VIEW
            values = SimporterDataUtil.list_date(values)

        attrs_values[attr] = list(values)

    attrs_values = {**additional_options, **attrs_values}

    return render_template(
        template_name_or_list=["api_info.html"],
        attrs_vals=attrs_values,
        example_url=EXAMPLE_URL)


@app.route("/api/timeline", methods=["GET"])
def api_timeline():
    start_date = request.args.get(START_DATE)
    end_date = request.args.get(END_DATE)
    data_type = request.args.get(TYPE)
    data_grouping = request.args.get(GROUPING)

    attrs_list = [ASIN, BRAND, ID, SOURCE, STARS]
    attrs = {}

    for attr in attrs_list:
        attrs[attr] = request.args.get(attr)

    # filtering data in dates
    data = SimporterDataUtil.filter_date_range(
        data=simporter_data,
        date_start=start_date,
        date_end=end_date)
    # filtering data by attrs values
    data = SimporterDataUtil.filter_category_value(
        data=data,
        **attrs)
    # formatting by type
    data = SimporterDataUtil.group_type_data(
        data=data,
        data_type=data_type)
    # grouping timelines
    list_data = SimporterDataUtil.timeline_group_data(
        data=data,
        group_type=data_grouping)

    data_json = SimporterDataUtil.data_events_nums_with_dates(
        list_data=list_data)
    data_json = {TIMELINE: data_json}
    data_json = json.dumps(data_json)

    return data_json


if __name__ == "__main__":
    app.run(debug=True)
