<?php

require "config.php";

try {
  $connection = new PDO("mysql:host=localhost", $username, $password);
  $connection->setAttribute(PDO::ATTR_EMULATE_PREPARES, 0);
  $connection->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
  $sql = file_get_contents("./data/init.sql");
  $result = $connection->exec($sql);
  echo "Database and table successfully created.\n";
}

catch(PDOException $error) {
  echo $error->getMessage();
}

