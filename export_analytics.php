<?php


require "config.php";

$connection = new PDO($dsn, $username, $password);
$connection->setAttribute(PDO::ATTR_EMULATE_PREPARES, 0);
$connection->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

$programAnalyticsFile = 'data/programs_analytics.csv';

$sql = "select * from programdata";

$statement = $connection->prepare($sql);
$statement->execute();


if ($statement->rowCount() > 0) {

  // Create a file pointer.
  $file = fopen($programAnalyticsFile, 'w');
  $delimiter = ",";

  // Set column headers for the csv file.
  $fields = array('Agency', 'Mission area', 'Division', 'Program', 'Program Category', 'URL', 'Domain', 'Description', 'Program Type', 'Cost', 'Reviewed By', "Visits", 'Page Views', 'Prior Live Visitors', 'Live Visitors', 'Updated Date');
  fputcsv($file, $fields, $delimiter);

  // Output each row of the data, format line as csv and write to file pointer.
  while ($row = $statement->fetch(PDO::FETCH_ASSOC)) {
    $lineData = array($row['agency'], $row['missionarea'], $row['division'], $row['program'], $row['programcategory'], $row['url'], $row['domain'], $row['description'], $row['programtype'], $row['cost'], $row['reviewed_by'], $row['visits'], $row['pageviews'], $row['priorlivevisitors'], $row['livevisitors'], $row['date']);
    fputcsv($file, $lineData, $delimiter);
  }
}

fclose($file);

echo "Program Analytics file is now available under ".$programAnalyticsFile . " \n";

