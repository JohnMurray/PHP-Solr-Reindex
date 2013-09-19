#!/usr/bin/env php

<?php
	/* Solr Connectivity */
	define('SOLR_HOST','http://0.0.0.0');			/* Hostname */
	define('SOLR_PORT','8983');						/* Port */
	define('SOLR_PATH','/solr/');					/* Default Solr path (usually /solr/) */
	define('SOLR_CORE','core1/');					/* Core */
	define('SOLR_WRITER_TYPE','json');				/* Writer Type - output type.  Currently only 'json' is supported. */
	
	/* Performance Options */
	define('PAGINATE_ROWS',1000);					/* Number of docs to show per page */
	define('COMMIT_FREQUENCY',10);					/* How many pages to commit after. */
	define('CURL_TIMEOUT',60);						/* How long until curl request times out.  Set to 0 for unlimited. */
	
	/* Re-Indexing Options */
	define('QUERY','*:*');							/* Initial query to use for searching/re-indexing */
	define('STARTINDEX',0);							/* Index to start searching/re-indexing from */
	define('ENDINDEX',0);							/* Index to end searching/re-indexing.  Set to 0 for no end index. */
	
	/* Query Parameters for search query */
	$params = array(
		'q' => QUERY,
		'start' => STARTINDEX,
		'rows' => PAGINATE_ROWS
	);
	
	/* Ignore Fields
		Format: 'field_name' => 'operation' (empty,ignore)
		Operations:
			empty - sets a blank value for the field ''
			ignore - does not re-post this fields data	
	*/
	$ignore_fields = array(
		
	);

  /* Set Timezone (for status messages) */
  date_default_timezone_set('America/New_York');
		
	/**********************************************************/
	// Class definitions - edit at your own risk
	/**********************************************************/
	
	class Solr {
		
		/**
	     *  executes a get query on a solr database
	     * 
	     *  @param array $query - associative array (key/value) of query string variables
	     *  @return array $data - solr data array
	     *  @throws none
	     */
		public static function get($query,$retry=null) {
			$ch = self::init_curl();	/* Initialize curl */

			//Set output type
			$query['wt'] = SOLR_WRITER_TYPE;

			//Set URL
			$url = SOLR_HOST . SOLR_PATH . SOLR_CORE . 'select?' . http_build_query($query);

			curl_setopt($ch, CURLOPT_URL, $url);
			
			$data = self::transform(curl_exec($ch));
			
			if(count($data['response']['docs']) > 0) {
				return $data;
			} else {
				//Sleep and re-try once
				if($retry !== true) {
					sleep(5);
					self::get($query,true);
				}				
				return false;
			}
		}

		/**
	     *  posts data to a solr database and optionally commits data
	     * 
	     *  @param array $data - associative (key/value pair) array of solr data
	     *  @return array $response - solr response array
	     *  @throws none
	     */
		public static function post($data) {
			$ch = self::init_curl();	/* Initialize curl */
			
			//Set update URL
			$url = SOLR_HOST . SOLR_PATH . SOLR_CORE . 'update/json';
			curl_setopt($ch, CURLOPT_URL, $url);
			
			//Configure curl for post
			curl_setopt($ch,CURLOPT_POSTFIELDS, json_encode($data));
			
			curl_setopt($ch,CURLOPT_POST,true);
			
			curl_setopt($ch,CURLOPT_HTTPHEADER, array(
				'Content-Type: application/json'
			));
			
			//Execute post
      // echo curl_exec($ch);
      // self::rollback();
      // exit(1);
			$solr_response = json_decode(curl_exec($ch));
						
			return (int) $solr_response->responseHeader->status;
		}

		/**
	     *  sends a commit statement to solr
	     * 
	     *  @param none
	     *  @return array $response - solr response array
	     *  @throws none
	     */
		public static function commit() {
			$ch = self::init_curl();	/* Initialize curl */

			$url = SOLR_HOST . SOLR_PATH . SOLR_CORE . 'update?commit=true';
			curl_setopt($ch, CURLOPT_URL, $url);
			
			curl_setopt($ch,CURLOPT_POST,true);
			
			//Execute post
			$solr_response = json_decode(curl_exec($ch));
						
			return (int) $solr_response->responseHeader->status;
		}

    public static function rollback() {
      $ch = self::init_curl();

      $url = SOLR_HOST . SOLR_PATH . SOLR_CORE . 'update?rollback=true';
      curl_setopt($ch, CURLOPT_URL, $url);

      curl_setopt($ch, CURLOPT_POST, true);

      //Execute post
			$solr_response = json_decode(curl_exec($ch));
						
			return (int) $solr_response->responseHeader->status;
    }

		
		/**
	     *  sends an optimize statement to solr
	     * 
	     *  @param none
	     *  @return array $response - solr response array
	     *  @throws none
	     */
		public static function optimize() {
			$ch = init_curl();	/* Initialize curl */

			$url = SOLR_HOST . SOLR_PATH . SOLR_CORE . '?optimize=true';
			curl_setopt($ch, CURLOPT_URL, $url);

      curl_setopt($ch, CURLOPT_POST, true);

      //Execute post
			$solr_response = json_decode(curl_exec($ch));
						
			return (int) $solr_response->responseHeader->status;
		}
		
		/**
	     *  transforms raw solr responses into associative array
	     * 
	     *  @param string $data - raw, unprocessed, solr data
	     *  @return array $response - associative array representation of raw solr response data
	     *  @throws Exception - unsupported output format
	     */
		public static function transform($data) {
			switch(SOLR_WRITER_TYPE) {
				case 'json':
					return json_decode($data,true);
				default:
					throw new Exception('Unsupported JSON output format.  Check SOLR_WRITER_TYPE.');
			}
		}

		/**
	     *  initializes curl with default options
	     * 
	     *  @param none
	     *  @return resource $ch - curl handle
	     *  @throws none
	     */
		protected static function init_curl() {
			//Initialize CURL
			$ch = curl_init();

			//Set CURL Options to return results
			curl_setopt ($ch, CURLOPT_RETURNTRANSFER, 1);

			//Follow up to two redirects
			curl_setopt($ch, CURLOPT_FOLLOWLOCATION, 1); 
			curl_setopt($ch, CURLOPT_MAXREDIRS, 2);

			//Set timeout so it doesn't run forever
			//5 seconds to make a connection
			//15 seconds for the whole transfer
			curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, CURL_TIMEOUT);
			curl_setopt($ch, CURLOPT_TIMEOUT, CURL_TIMEOUT);

			//Do not verify SSL peer
			curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);

			//Set connectivity port
			curl_setopt($ch, CURLOPT_PORT, SOLR_PORT);

			return $ch;
		}
	}
		
	/**********************************************************/
	// Procedural execution steps - edit at your own risk
	/**********************************************************/
	
	/* Main application loop */	
	$doc_cnt=$params['start'];
	$page_cnt=0;
	$total_docs=0;
	
	while(($data = Solr::get($params)) !== false) {
		$total_docs = $data['response']['numFound'];
		$page_cnt++;
		
		//Initialize solr wrapper
		$solr_array = array();
		
		//Loop through returned documents
		foreach($data['response']['docs'] as $doc) {
			//Increment document count
			$doc_cnt++;
						
			//See if ENDINDEX has been reached
			$end_of_index = (ENDINDEX > 0 && $doc_cnt == ENDINDEX);
			
			//Remove un-wanted fields
			foreach($ignore_fields as $i_field=>$operation) {
				if(array_key_exists($i_field,$doc) === true) {
					switch($operation) {
						case 'empty':
							$doc[$i_field] = '';
							break;
						case 'ignore':
							unset($doc[$i_field]);
							break;
					}
				}
			}
						
			//Append data to solr array			
      $solr_array[] = $doc;
			
			
			//Stop if we reached the end of the index
			if($end_of_index === true) {
				//Force next pagination request to end
				$params['start'] = $total_docs;
				break;
			}			
		} //foreach docs
		
		//Post page of data
		$response = Solr::post($solr_array);
		if($response !== 0) {
			echo "Error processing document ID: " . $doc['id'] . "\.  Response: $response.\n";
		}
		
		//Commit page(s) of data
		if($page_cnt % COMMIT_FREQUENCY == 0 || $end_of_index) {
			$response = Solr::commit();
			
			if($response !== 0) {
				echo "Error committing data to Solr!  Response: $response.\n";
			} else {
				echo "Committed Data - $doc_cnt of $total_docs (" . date("c") . ") " . number_format(($doc_cnt/$total_docs)*100,4) . "% Complete.\n";
			}
		}
		
		//Increment
		$params['start'] += PAGINATE_ROWS;
	}
