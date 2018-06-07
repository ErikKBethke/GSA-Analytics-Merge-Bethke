<?php

echo "Step 1: Create the required database and tables \n";
require "install.php";

echo "Step 2: Import the program Data and Analytics into the Database \n";
require "import_analytics.php";

echo "Step 3: Export the merged data from database to a csv file. \n";
require "export_analytics.php";

