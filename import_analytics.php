<?php


require "config.php";

$connection = new PDO($dsn, $username, $password);
$connection->setAttribute(PDO::ATTR_EMULATE_PREPARES, 0);
$connection->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

$programFile = 'data/programs.csv';
$program_data = readCSV($programFile, false);
// Grab first row of CSV, which holds the header values.
$program_data_header = array_shift($program_data);

$all_domains_30_days = readCSV('https://analytics.usa.gov/data/agriculture/all-domains-30-days.csv', true);
// Grab first row of CSV, which holds the header values.
$all_domains_30_days_header = array_shift($all_domains_30_days);

// Convenient to store these indexes rather than searching for them each time.
$domain_index = array_search('domain', $all_domains_30_days_header);
$visits_index = array_search('visits', $all_domains_30_days_header);
$pageviews_index = array_search('pageviews', $all_domains_30_days_header);

// Truncate programdata table to remove existing data.
$sql1 = "TRUNCATE TABLE `programdata`";
$statement1 = $connection->prepare($sql1);
$statement1->execute();

foreach ($program_data as $program) {
    $row = array();
    $row['agency'] = isset($program[array_search('Agency', $program_data_header)]) ? $program[array_search('Agency', $program_data_header)] : NULL;
    $row['missionarea'] = isset($program[array_search('Mission Area', $program_data_header)]) ? $program[array_search('Mission Area', $program_data_header)] : NULL;
    $row['division'] = isset($program[array_search('Office/Division/National Program', $program_data_header)]) ? $program[array_search('Office/Division/National Program', $program_data_header)] : NULL;
    $row['program'] = isset($program[array_search('Program', $program_data_header)]) ? $program[array_search('Program', $program_data_header)] : NULL;
    $row['programcategory'] = isset($program[array_search('Program Category', $program_data_header)]) ? $program[array_search('Program Category', $program_data_header)] : NULL;
    $row['url'] = isset($program[array_search('URL', $program_data_header)]) ? $program[array_search('URL', $program_data_header)] : NULL;
    $row['domain'] = isset($program[array_search('Domain', $program_data_header)]) ? $program[array_search('Domain', $program_data_header)] : NULL;
    $row['description'] = isset($program[array_search('Description', $program_data_header)]) ? $program[array_search('Description', $program_data_header)] : NULL;
    $row['programtype'] = isset($program[array_search('Program Types', $program_data_header)]) ? $program[array_search('Program Types', $program_data_header)] : NULL;
    $row['cost'] = isset($program[array_search('Cost/Budget', $program_data_header)]) ? $program[array_search('Cost/Budget', $program_data_header)] : NULL;
    $row['reviewed_by'] = isset($program[array_search('Reviewed By', $program_data_header)]) ? $program[array_search('Reviewed By', $program_data_header)] : NULL;

    // Filter $all_domains_30_days to grab just the row with the current domain.
    $this_domain_30_days = array_values(
        array_filter(
            $all_domains_30_days, function ($v, $k) use ($domain_index, $row) {
                return $v[$domain_index] == $row['domain'];
            },
        ARRAY_FILTER_USE_BOTH)
    );

    if (!empty($this_domain_30_days)) {
        $row['visits'] = isset($this_domain_30_days[0][$visits_index]) ? $this_domain_30_days[0][$visits_index] : NULL;
        $row['pageviews'] = isset($this_domain_30_days[0][$pageviews_index]) ? $this_domain_30_days[0][$pageviews_index] : NULL;
    }

    $sql2 = sprintf(
        "INSERT INTO %s (%s) values (%s)",
        "programdata",
        implode(", ", array_keys($row)),
        ":" . implode(", :", array_keys($row))
    );

    $statement2 = $connection->prepare($sql2);
    $statement2->execute($row);
}

/**
 * Parse a CSV file and store as an array.
 * @param string $csvFile
 *    Can be local file or remore URL.
 * @param bool $gzipped'
 *    Is the file gzip encoded?
 *
 * @return array
 *    The parsed CSV data
 */
function readCSV($csvFile, $gzipped){
    $file_handle = $gzipped ? gzopen($csvFile, 'rb') : fopen($csvFile, 'rb');
    while (!feof($file_handle) ) {
        $line_of_text[] = fgetcsv($file_handle, 1024);
    }
    fclose($file_handle);
    return $line_of_text;
}
