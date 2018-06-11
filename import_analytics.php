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
$all_domains_30_days_header = array_shift($all_domains_30_days);

$all_pages_realtime = readCSV('https://analytics.usa.gov/data/agriculture/all-pages-realtime.csv', true);
$all_pages_realtime_header = array_shift($all_pages_realtime);

// Convenient to store these indexes rather than searching for them each time.
$domain_index = array_search('domain', $all_domains_30_days_header);
$visits_index = array_search('visits', $all_domains_30_days_header);
$pageviews_index = array_search('pageviews', $all_domains_30_days_header);
$page_index = array_search('page', $all_pages_realtime_header);
$active_visitors_index = array_search('active_visitors', $all_pages_realtime_header);


// Truncate programdata table to remove existing data.
try {
  $sql1 = "UPDATE `programdata` set priorlivevisitors=livevisitors";
  $statement1 = $connection->prepare($sql1);
}
catch(Exception $e) {
  echo $e->getMessage();die;
}

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
  $row['visits'] = 0;
  $row['pageviews'] = 0;

  // Filter $all_domains_30_days to grab just the row with the current domain.
  $this_domain_30_days = array_values(
    array_filter(
      $all_domains_30_days, function ($v, $k) use ($domain_index, $row) {
      return $v[$domain_index] == $row['domain'];
    },
      ARRAY_FILTER_USE_BOTH)
  );

  if (!empty($this_domain_30_days)) {
    $row['visits'] = isset($this_domain_30_days[0][$visits_index]) ? $this_domain_30_days[0][$visits_index] : 0;
    $row['pageviews'] = isset($this_domain_30_days[0][$pageviews_index]) ? $this_domain_30_days[0][$pageviews_index] : 0;
  }

  $this_page_realtime = array_values(
    array_filter(
      $all_pages_realtime, function ($v, $k) use ($page_index, $row) {
        $parsed_page = parse_page_from_url($row['url']);
        return $v[$page_index] == $parsed_page || $v[$page_index] == $parsed_page . '/';
    },
      ARRAY_FILTER_USE_BOTH)
  );

  if (!empty($this_page_realtime)) {
    $row['livevisitors'] = isset($this_page_realtime[0][$active_visitors_index]) ? $this_page_realtime[0][$active_visitors_index] : NULL;
  }

  /**
   * Callback function for array_map function to prepare update statement.
   *
   * This takes the $row array and turns it into an array of statements in the
   * form key=value, suitable for a SQL "UPDATE" statement.
   *
   * @param string $key
   *    Column name.
   * @param string $value
   *    Column value.
   *
   * @return string
   *    in the form $key = '$value'.
   */
  $setupdates = function($key, $value) use ($connection){
    return $key . "=" . $connection->quote($value);
  };

  $sql3 = sprintf(
    "INSERT INTO %s (%s) values (%s) ON DUPLICATE KEY UPDATE %s",
    "programdata",
    implode(", ", array_keys($row)),
    ":" . implode(", :", array_keys($row)),
    implode(', ', array_map($setupdates, array_keys($row), $row))
  );

  $statement3 = $connection->prepare($sql3);
  $statement3->execute($row);
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

function parse_page_from_url($url) {
  $split_string = explode('//', $url);
  if (empty($split_string[1])) {
    return $url;
  }
  $parsed_url = $split_string[1];
  if (strpos($parsed_url, 'www.') !== FALSE) {
    $parsed_url = substr($parsed_url, strpos($parsed_url, 'www.') + 4);
  }
  return $parsed_url;
}
