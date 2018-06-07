<?php


require "config.php";

$connection = new PDO($dsn, $username, $password);
$connection->setAttribute(PDO::ATTR_EMULATE_PREPARES, 0);
$connection->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

$programFile = 'data/programs.csv';
$program_data = readCSV($programFile, false);
$program_data_header = array_shift($program_data);

$all_domains_30_days = readCSV('https://analytics.usa.gov/data/agriculture/all-domains-30-days.csv', true);
$all_domains_30_days_header = array_shift($all_domains_30_days);

$domain_index = array_search('domain', $all_domains_30_days_header);
$visits_index = array_search('visits', $all_domains_30_days_header);
$pageviews_index = array_search('pageviews', $all_domains_30_days_header);

// Truncate programdata table to remove existing data.
$sql1 = "TRUNCATE TABLE `programdata`";
$statement1 = $connection->prepare($sql1);
$statement1->execute();


foreach ($program_data as $program) {
    $row = array();
    $row['missionarea'] = $program[array_search('Mission Area', $program_data_header)];
    $row['division'] = $program[array_search('Office/Division/National Program', $program_data_header)];
    $row['program'] = $program[array_search('Program', $program_data_header)];
    $row['programcategory'] = $program[array_search('Program Category', $program_data_header)];
    $row['url'] = $program[array_search('URL', $program_data_header)];
    $row['domain'] = $program[array_search('Domain', $program_data_header)];
    $row['description'] = $program[array_search('Description', $program_data_header)];
    $row['programtype'] = $program[array_search('Program Types', $program_data_header)];
    $row['cost'] = $program[array_search('Cost/Budget', $program_data_header)];
    $row['reviewed_by'] = $program[array_search('Reviewed By', $program_data_header)];

    $this_domain_30_days = array_values(
        array_filter(
            $all_domains_30_days, function ($v, $k) use ($domain_index, $row) {
                return $v[$domain_index] == $row['domain'];
            },
        ARRAY_FILTER_USE_BOTH)
    );


    if (!empty($this_domain_30_days)) {
        $row['visits'] = $this_domain_30_days[0][$visits_index];
        $row['pageviews'] = $this_domain_30_days[0][$pageviews_index];
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


function readCSV($csvFile, $gzipped){
    $file_handle = $gzipped ? gzopen($csvFile, 'rb') : fopen($csvFile, 'rb');
    while (!feof($file_handle) ) {
        $line_of_text[] = fgetcsv($file_handle, 1024);
    }
    fclose($file_handle);
    return $line_of_text;
}
