const functions = require('@google-cloud/functions-framework');
const { queryFBMetrics, queryFBMetricsbyDateRange } = require('./query');

functions.http('getUserMetrics', async (req, res) => {
    console.log('===================BEGIN getUserMetrics======================');
    const queryParams = Object.fromEntries(
        Object.entries(req.query).map(([k, v]) => [k.toLowerCase(), v]),
    );
    const { fromdate, todate, date } = queryParams;

    console.log('fromdate=', fromdate);
    console.log('todate=', todate);
    console.log('date=', date);
    let result = null;

    if (fromdate && todate) {
        result = await queryFBMetricsbyDateRange(fromdate, todate);
    } else if (date) {
        result = await queryFBMetrics(date);
    } else {
        result = {
            status: 'error',
            body: 'Invalid request. Incorrect parameters passed.',
        };
    }
    console.log(result);

    if (result.status === 'success') {
        res.send(result.body);
    } else {
        res.status(400).send(result.body);
    }
});
