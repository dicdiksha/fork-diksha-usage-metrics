const { queryFBMetrics, queryFBMetricsbyDateRange } = require("./query");

async function test(fromDate, toDate) {
    let result = null;

    if (fromDate && toDate) {
        result = await queryFBMetricsbyDateRange(fromDate, toDate);
    } else if (fromDate) {
        result = await queryFBMetrics(fromDate);
    } else {
        result = await queryFBMetrics('20230802');   
    }
    console.log(result);
    return result;
}

test(fromDate=null, toDate=null);