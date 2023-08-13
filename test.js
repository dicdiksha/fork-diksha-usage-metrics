const { queryFBMetrics, queryFBMetricsbyDateRange } = require('./query');

async function test() {
    let result = null;
    const fromDate = process.argv[2];
    const toDate = process.argv[3];

    console.log('fromdate=', fromDate);
    console.log('todate=', toDate);

    if (fromDate && toDate) {
        result = await queryFBMetricsbyDateRange(fromDate, toDate);
    } else if (fromDate) {
        result = await queryFBMetrics(fromDate);
    } else {
        result = {
            status: 'error',
            body: 'Invalid request. Incorrect parameters passed.',
        };
    }
    console.log(result);
    return result;
}

test();
