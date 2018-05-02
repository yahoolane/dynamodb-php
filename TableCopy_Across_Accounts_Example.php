Do your init here. of the library. 


Now in this great example, we are going to do the unimangable

we are going to copy a table from 1 instance of AWS to another.. 2 different Accounts.

in this example you need to create the table on the 2nd instance. with the key exactly like the original table
but you can change other settings as needed. 
//  https://www.paypal.me/LaneFowler  Please donate. 


$ddb = new DynamoDBWrapper(array(
   'version' => latest,
 
   'region' => 'us-east-1',
   
 'credentials' => array(
    'key'    => 'KEY_instance1',
    'secret' => 'Secret_KEY_Instance1'
  )
     
));

$ddb2 = new DynamoDBWrapper(array(
   'version' => latest,
 
   'region' => 'us-east-1',
   
 'credentials' => array(
    'key'    => 'KEY_instance2',
    'secret' => 'SECRET_KEY_instance2'
  )
     
));

// I have used this to copy tables of about 1000 records. 

$tablename="Online";  // Put your table name here. 
$result = $ddb->scanWT($tablename, '' );

   //echo "<pre>";
   // echo print_r($result,true); // this will show you the entire table, or until the buffer is full. 
   //echo "</pre><hr><br>get<br>";


// We are doing 1 Record at a time.  just in case we need to debug. 
// it may take longer, but it works well. 

    foreach ($result as $row) {
   //     echo print_r($row,true);  // print here if needed
     echo $result2 = $ddb2->Put($tablename,$row );
 
    }



// we are done  




