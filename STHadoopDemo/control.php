<?php

if ($_POST['action']=="view") {
    echo $_POST['node']."/".$_POST['edge'];

}

if ($_POST['action']=="coords") {

$type = $_POST['type'];
    switch ($type) {
        case 'Road Network':
            $table = 'road_edges';
            break;
        case 'Lakes':
            $table = 'lake_edges';
            break;
        case 'Rivers':
            $table = 'river_edges';
            break;
        case 'Borders - National\Country':
            $table = 'national_edges';
            break;
        case 'Borders - State\Region\Province':
            $table = 'region_edges';
            break;
        case 'Borders - County\District\Prefectures':
            $table = 'district_edges';
            break;
        case 'Borders - City\Municipality\Town':
            $table = 'city_edges';
            ;break;
        case 'Borders - Neighborhood\Suburb':
            $table = 'neighborhood_edges';
            ;break;
        case 'Borders - All':
            $table = 'border_edges';
            ;break;
        case 'Parks':
            $table = 'park_edges';
            ;break;
        case 'Buildings - Residential':
            $table = 'resident_edges';
            ;break;
        case 'Buildings - Commercial':
            $table = 'commerce_edges';
            ;break;
        case 'Buildings - Services':
            $table = 'services_edges';
            ;break;
        case 'Buildings - Sport':
            $table = 'sport_edges';
            ;break;
        case 'Buildings - Schools':
            $table = 'Education_edges';
            ;break;
        case 'Buildings - Worship':
            $table = 'worship_edges';
            ;break;
        case 'Buildings - All':
            $table = 'building_edges';
            ;break;
	case 'Cemetery':
            $table = 'cemetery_edges';
            ;break;
	case 'desert':
            $table = 'desert_edges';
            ;break;
	case 'Borders - region':
            $table = 'region_edges';
            ;break;
	case 'Borders - district':
            $table = 'district_edges';
            ;break;
	case 'Borders - city':
            $table = 'city_edges';
            ;break;
	case 'Borders - administrative':
            $table = 'administrative_edges';
            ;break;
	case 'Borders - postal':
            $table = 'postal_edges';
            ;break;
	case 'Borders - maritime':
            $table = 'maritime_edges';
            ;break;
	case 'Borders - political':
            $table = 'political_edges';
            ;break;
	case 'Borders - national':
            $table = 'national_edges';
            ;break;
        case 'Border - Coast line':
            $table = 'coast_edges';
            break;

    }

$coords = split(',', $_POST['coords']);
$latmax = $coords[0];
$latmin = $coords[1];
$lonmax = $coords[2];
$lonmin = $coords[3];

//$rName = $_POST['rName'];
$rName = 'TareeqDemo';
$rEmail = $_POST['rEmail'];
   
//args in the jar as following name  of the user, email, type , location, datafolder , exportfolder, email folder
exec("rm -rf /home/yackel/public_html/app/webroot/tareeq/data/error.csv");
exec("rm -rf /home/yackel/public_html/app/webroot/tareeq/data/log.csv");
exec("rm -rf /home/yackel/public_html/app/webroot/tareeq/data/report");
exec("rm -rf /home/yackel/public_html/app/webroot/tareeq/data/1");
//exec("chmod ugo+rwx /home/yackel/public_html/app/webroot/tareeq/data");
#exec("java -jar /home/yackel/public_html/app/webroot/tareeq/OSMExtractor.jar $rName $rEmail $table $latmax $lonmax $latmin $lonmin /media/osmeDataset/spatialHadoopPhase/ /home/yackel/public_html/app/webroot/downloads/userData/ /media/Louai/usersData/email/ 1");
exec("java -jar /home/yackel/public_html/app/webroot/tareeq/OSMExtractor.jar $rName $rEmail $table $latmax $lonmax $latmin $lonmin /media/osmeDataset/spatialHadoopPhase/ /home/yackel/public_html/app/webroot/tareeq/data/ /home/yackel/public_html/app/webroot/tareeq/data/ 0| cat > /home/yackel/public_html/app/webroot/tareeq/data/report");
sleep(3);
exec("chmod ugo+rwx /home/yackel/public_html/app/webroot/tareeq/data/1");
exec("chmod ugo+rwx /home/yackel/public_html/app/webroot/tareeq/data/1/edge.txt");
exec("chmod ugo+rwx /home/yackel/public_html/app/webroot/tareeq/data/1/unsortedNode.txt");
exec("chmod ugo+rwx /home/yackel/public_html/app/webroot/tareeq/data/1/log.txt");
exec("sh ./home/yackel/public_html/app/webroot/tareeq/getNode.sh /home/yackel/public_html/app/webroot/tareeq/data/1/unsortedNode.txt /home/yackel/public_html/app/webroot/tareeq/data/1/node.txt");
exec("chmod ugo+rwx /home/yackel/public_html/app/webroot/tareeq/data/1/node.txt");
//sleep(6);

// *************Query from Database************
$db_host = 'localhost';
$db_user = 'traffic';
$db_pwd = 'thetrafficgenerator';
$database = 'traffic';

// Connect to database
if (!mysql_connect($db_host, $db_user, $db_pwd))
    die("Can't connect to database");

if (!mysql_select_db($database))
    die("Can't select database");

// Create tables 
/*
$sql="CREATE TABLE edge (edge_id VARCHAR(40),node_1 VARCHAR(40),node_2 VARCHAR(40),tags VARCHAR(200));";
if(!mysql_query($sql)){
	die('could not crate edge tabel');
	print "could not careate edge table";
}else{print "node table created\n";}

$sql="CREATE TABLE node (id VARCHAR(40),lat VARCHAR(40),lon VARCHAR(40));";
if(!mysql_query($sql)){
	die('could not crate node table ');
	echo "could not careate node table";
}else{print "edge table created\n";}
*/


// load node data to Database
$i=0;
if (($handle = fopen("/home/yackel/public_html/app/webroot/tareeq/data/1/node.txt", "r")) !== FALSE) { 

        while (($data = fgetcsv($handle, 1000, ",")) !== FALSE) { 
	$i++;
        if($i <> 1){   
//		 $sql = "INSERT INTO node (id,lat,lon) VALUES ('$data[0]','$data[1]','$data[2]');"; 
	         $nodes .="$data[0],$data[1],$data[2];";
//        	    if(!mysql_query($sql)){
//        		die('could not crate edge tabel');
//        		print "could not insert values to node table";
//	    	}else{
			//print "nodes has been inserted\n";
//       		}//end else
	 }
        } //if header

        fclose($handle); 
    }
// load edge data to Database 
$i=0;
if (($handle = fopen("/home/yackel/public_html/app/webroot/tareeq/data/1/edge.txt", "r")) !== FALSE) { 

        while (($data = fgetcsv($handle, 1000, ",")) !== FALSE) { 
	   $i++;
 	   if($i <> 1){
//           	 $sql = "INSERT INTO edge (edge_id,node_1,node_2,tags) VALUES ('$data[0]','$data[1]','$data[2]','$data[3]');"; 
		$edges .="$data[0],$data[1],$data[2];";
//            	if(!mysql_query($sql)){
//                        die('could not crate edge tabel');
//                        print "could not insert values to node table";
//           	 }else{
              	     //print "edge has been inserted\n";
//           	 }
 	    }//end if 
        } 
        fclose($handle); 
    }


// Query data base and delete
/*
// Query database for nodes 
// sending query
$result = mysql_query("SELECT * FROM node");
if (!$result) {
    die("Query to show fields from table failed");
}

    $file = fopen("/home/yackel/public_html/app/webroot/tareeq/node.txt", "w");
    fwrite($file, "node_id, lat, lon\n");
    while ($row = mysql_fetch_row($result)) {
        $n++;
        $nodes .="$row[0],$row[1],$row[2];";
        fwrite($file, "$row[0],$row[1],$row[2]\n");
    }
	fclose($file);

// Query database for edges
$result = mysql_query("SELECT * FROM edge");
if (!$result) {
    die("Query to show fields from table failed");
}

    $file = fopen("/home/yackel/public_html/app/webroot/tareeq/edge.txt", "w");
    fwrite($file, "edge_id, from_node, to_node\n");
    while ($row = mysql_fetch_row($result)) {
        $e++;
        $edges .="$row[0],$row[1],$row[2];";
        fwrite($file, "$row[0],$row[1],$row[2],$row[3]\n");
    }
fclose($file);


// delete data from database
$sql = "DELETE FROM node";
if(!mysql_query($sql))   
        die('could not delete nodes from database');
$sql = "DELETE FROM edge";
if(!mysql_query($sql))   
        die('could not delete edges from database');

 mysql_close($conn);

*/


//exec("rm -rf /home/yackel/public_html/app/webroot/tareeq/data/*");
 echo $nodes."/".$edges."/".$type;

 }
?>
