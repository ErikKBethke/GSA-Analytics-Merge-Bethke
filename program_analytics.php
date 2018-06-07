<?php

echo "Step 1: Create the required database and tables";
require "install.php";

echo "Step 2: Import the program Data and Analytics into the Database";
require "import_analytics.php";

echo "Step 3: Export the merged data from database to a csv file.";
require "export_analytics.php";

