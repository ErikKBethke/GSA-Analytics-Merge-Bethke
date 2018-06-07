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
    $fields = array('ID', 'Mission area', 'Division', 'Program', 'Program Category', 'URL', 'Domain', 'Description', 'Program Type', 'Cost', 'Reviewed By', "visits", 'Page Views', 'Updated Date');
    fputcsv($file, $fields, $delimiter);

    // Output each row of the data, format line as csv and write to file pointer.
    while ($row = $statement->fetch(PDO::FETCH_ASSOC)) {
        $lineData = array($row['id'], $row['missionarea'], $row['division'], $row['program'], $row['programcategory'], $row['url'], $row['domain'], $row['description'], $row['programtype'], $row['cost'], $row['reviewed_by'], $row['visits'], $row['pageviews'], $row['date']);
        fputcsv($file, $lineData, $delimiter);
    }
  
    // Move back to beginning of file
    fseek($file, 0);

    // Set the required headers to for the csv file.
    header('Content-Type: text/csv');
    header('Content-Disposition: attachment; filename="' . $programAnalyticsFile . '";');
    header("Pragma: no-cache");

    // Output all the remaining data onto a file pointer.
    fpassthru($file);

}

fclose($file);

echo "Program Analytics file is now available under ".$programAnalyticsFile;

