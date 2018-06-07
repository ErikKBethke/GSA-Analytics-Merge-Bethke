<?php

//$host = "localhost";
$host = "127.0.0.1";
$username = "root";
$password = "root";
$dbname = "program_analytics";
$dsn = "mysql:host=$host;dbname=$dbname";
$options = array(
  PDO::ATTR_ERRMODE =>PDO::ERRMODE_EXCEPTION
);
