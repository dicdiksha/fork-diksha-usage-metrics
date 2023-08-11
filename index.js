const functions = require('@google-cloud/functions-framework');
// const { BigQuery } = require('@google-cloud/bigquery');
// const bigquery = new BigQuery();

const { queryFBMetrics, queryFBMetricsbyDateRange } = require("./query");

functions.http('getUserMetrics', async (req, res) => {
    //res.send(`Hello ${req.query.name || req.body.name || 'World'}!`);
    console.log('========================BEGIN helloHttp++++++++++++++++++++++++');
    console.log('req.url=', req.url);
    const queryParams = req?.url?.split('&');
    const fromDate = queryParams?.[1]?.split('=')?.[1];
    const toDate = queryParams[2]?.split('=')?.[1];
    console.log('fromDate=', fromDate);
    console.log('toDate=', toDate);
    let result = null;

    if (fromDate && toDate) {
        result = await queryFBMetricsbyDateRange(fromDate, toDate);
    } else {
        result = await queryFBMetrics(fromDate);
    }
    console.log(result);

    res.send(result);
});