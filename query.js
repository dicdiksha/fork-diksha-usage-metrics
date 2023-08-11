const { BigQuery } = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
const { TABLE_PREFIX } = require("./utils/constant");

const queryFBMetrics = async (fromDate, toDate) => {
    console.log('========================BEGIN queryFBMetrics----------------------------');
    console.log(`fromDate=${fromDate} toDate=${toDate}`);
    const metrics = {};

    const activeUsersQuery = `SELECT COUNT(DISTINCT user_pseudo_id) AS active_users_count FROM   \`diksha-4446b.analytics_163027146.events_20230802\` AS T
      CROSS JOIN T.event_params 
      WHERE event_params.key = 'engagement_time_msec' AND event_params.value.int_value > 0
    -- Pick events in the last N = 20 days.
      AND event_timestamp > UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 20 DAY))`;

    // For all options, see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
    const activeUsersOptions = {
        query: activeUsersQuery,
        // Location must match that of the dataset(s) referenced in the query.
        // location: 'asia',
    };

    // Run the query as a job
    const [job] = await bigquery.createQueryJob(activeUsersOptions);
    console.log(`Job ${job.id} for active users started.`);

    // Wait for the query to finish
    const [activeUsersResult] = await job.getQueryResults();

    // Print the results
    console.log('activeUsersResult:');
    console.log(activeUsersResult?.[0]);

    //Update the metrics
    Object.assign(metrics, activeUsersResult[0]);

    const newUsersQuery = `select count (distinct user_pseudo_id) AS new_users_count FROM  \`diksha-4446b.analytics_163027146.events_20230802\` 
      where event_name = 'first_open'`;

    const newUsersOptions = {
        query: newUsersQuery,
        // Location must match that of the dataset(s) referenced in the query.
        // location: 'asia',
    };

    // Run the query as a job
    const [newUsersJob] = await bigquery.createQueryJob(newUsersOptions);
    console.log(`Job ${newUsersJob.id} for new users started.`);

    // Wait for the query to finish
    const [newUsersResult] = await newUsersJob.getQueryResults();

    // Print the results
    console.log('newUsersResult:');
    console.log(newUsersResult?.[0]);

    //Update the metrics
    Object.assign(metrics, newUsersResult[0]);

    return metrics;
}

const queryFBMetricsbyDateRange = async (fromDate, toDate) => {
    console.log('========================BEGIN queryFBMetrics----------------------------');
    console.log(`fromDate=${fromDate} toDate=${toDate}`);
    const metrics = {};

    const activeUsersQuery = `SELECT COUNT(DISTINCT user_pseudo_id) AS active_users_count FROM   \`diksha-4446b.analytics_163027146.events_20230802\` AS T
      CROSS JOIN T.event_params 
      WHERE event_params.key = 'engagement_time_msec' AND event_params.value.int_value > 0
    -- Pick events in the last N = 20 days.
      AND event_timestamp > UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 20 DAY))`;

    // For all options, see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
    const activeUsersOptions = {
        query: activeUsersQuery,
        // Location must match that of the dataset(s) referenced in the query.
        // location: 'asia',
    };

    // Run the query as a job
    const [job] = await bigquery.createQueryJob(activeUsersOptions);
    console.log(`Job ${job.id} for active users started.`);

    // Wait for the query to finish
    const [activeUsersResult] = await job.getQueryResults();

    // Print the results
    console.log('activeUsersResult:');
    console.log(activeUsersResult?.[0]);

    //Update the metrics
    Object.assign(metrics, activeUsersResult[0]);

    const newUsersQuery = `select count (distinct user_pseudo_id) AS new_users_count FROM  \`diksha-4446b.analytics_163027146.events_20230802\` 
      where event_name = 'first_open'`;

    const newUsersOptions = {
        query: newUsersQuery,
        // Location must match that of the dataset(s) referenced in the query.
        // location: 'asia',
    };

    // Run the query as a job
    const [newUsersJob] = await bigquery.createQueryJob(newUsersOptions);
    console.log(`Job ${newUsersJob.id} for new users started.`);

    // Wait for the query to finish
    const [newUsersResult] = await newUsersJob.getQueryResults();

    // Print the results
    console.log('newUsersResult:');
    console.log(newUsersResult?.[0]);

    //Update the metrics
    Object.assign(metrics, newUsersResult[0]);

    return metrics;
}

module.exports = {queryFBMetrics, queryFBMetricsbyDateRange}; 