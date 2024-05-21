/*
 * Generic GSheet Ingestion Service Documentation
 * ==============================================
 *
 * Overview:
 * The Generic GSheet Ingestion Service automates ingesting data from selected Google Sheets 
 * into the Harbor DB on PostgreSQL. It targets sheets on the Data Analytics Public Google Drive
 * and runs at regular intervals.
 *
 * Service Initialization:
 * - Automatic launch: Initiated using `start_gsheets.bat`, located at `c:\dataops_api\`.
 * - Trigger: Automatically executed at system startup via Windows Task Scheduler.
 *
 * Harbor connection:
 * - The service uses the 'api_rw__gsheets' system user on Harbor, which has write and create privledges 
 *   for the harbor.raw-gsheets schema. 
 *   The password is stored in an Environment Variable: pg_harbor_gsheets_api_user_password
 *
 * Google API credentials:
 * The API key JSON file is stored in C:\dataops_api\keys\
 * An Environent Variable is set in Windows, to point to the key file.
 *
 * GSheet Qualification Criteria:
 * Sheets must contain and fill out specific named ranges to be eligible:
 *   - `metadata_range` 	- points to a 2 col table specifying field names, datatypes
 *   - `data_range` 		- points to a table containing the data for ingestion. Header row is excluded
 *   - `table_name`			- specifyies the target table name, to be deployed on 'harbor.raw_gsheets' schema
 *   - `drop_table`			- TRUE: drop and recreate table, FALSE: truncate table before ingestion
 *   - `first_ingestion`    - Date for first ingestion, and the baseis for deriving periodic ingestion intervals
 *   - `ingestion_type`		- Automatic: ingestion runs periodically, Manual: skipped by the ingestion service.
 *   - `frequency_unit`		- Ingestion frequency unit: Minute, Hour, Day, Week, Month
 *   - `frequency_value`	- Ingestion frequency interval (per the selected frequency units).
 * 
 * Users should use the pre-built template, with predefined 'Setup' and 'Metadata' sheets, here: 
 * https://docs.google.com/spreadsheets/d/1dSJ_ukIrPrtBZf4EXq9reQFXFPEeDFGr8R9demKO5Ck/edit#gid=1326950837
 *   - Metadata sheet: contains a list of all fields and their datatypes.
 *   - Setup sheet: contains the setup named rabges, as specified above.
 *
 * Ingestion Cycle Triggering Logic:
 * - Routine cycles occur per a predefined time interval (default: every minute).
 * - Default cycle interval can be adjusted in the `main()` function.
 *
 * Ingestion Cycle Execution Logic:
 * - Each cycle starts by creating a `processID` variable as the current timestamp 
 *   (seconds, milliseconds set to zero).
 * - Eligibility: Checks if the GSheet is set up for Generic Ingestion. Skips non-eligible sheets.
 * - Ingestion Type: Skips GSheets if `ingestion_type` is set to 'Manual'.
 * - Timing: Determines go/no-go for each sheet based on `first_ingestion`, `frequency_unit`, 
 *   and `frequency_value`.
 * - Table Management:
 *   - Target table defined by 'table_name' named range, located in 'harbor.raw_gsheets' schema.
 *   - If `drop_table` is `[TRUE]`, the target table in Harbor is dropped and recreated.
 *   - If `drop_table` is `[FALSE]`, the target table is truncated.
 * - Data Insertion: Inserts data from the GSheet into the target table.
 *
 * Logging:
 * - Ingestion attempts are logged in the harbor.raw_gsheets.gsheet_ingestion_log table, including:
 *   - `gsheet_id` (Google file identifier)
 *   - `process_id` (cycle timestamp)
 *   - `log_entry_desc` (details on the performed operation: table creation, inserting data)
 *   - `log_entry_timestamp` (operation execution timestamp)
 *   - `log_entry_category` (info for successful operation, err for failed operation)
 *   - `err_message` (blank for successful operation, node js error message for a failed one)
 *
 * Versions:
 * - v1.0: 
 *   - Desc: MVP - proof of concept.
 *   - Developed by Evgeni Hasin, 11.01.2024.
 * 
 * Note: For further information or support, please contact the DataOps team.
 */

const { google } = require('googleapis');
const { Pool } = require('pg');
const log_table_name = 'raw_gsheets.gsheet_ingestion_log'; // Target tables will be deployed in the harbor.raw_sheets schema
const batchSize = 100; // Number of records to insert in one batch
process.env.TZ = 'Europe/Berlin'; // Set timezone to Europe / Berlin

// Ignore module deprecation warnings
process.on('warning', (warning) => {
    if (warning.name === 'DeprecationWarning') {
        // Ignore deprecation warnings
    } else {
        // Handle or log other types of warnings
        console.warn(warning.name, warning.message);
    }
});

// PostgreSQL connection settings
const pool = new Pool({
    user: 'api_rw__gsheets',
    host: 'pg-wefox-lucanet-pro.c8ci56qnraew.eu-central-1.rds.amazonaws.com',
    database: 'harbor',
    password: process.env.pg_harbor_gsheets_api_user_password,
    port: 5432,
});

// Assuming you have set the environment variable GOOGLE_APPLICATION_CREDENTIALS
async function authorizeGoogleServices() {
    const auth = new google.auth.GoogleAuth({
        scopes: [
            'https://www.googleapis.com/auth/spreadsheets.readonly',
            'https://www.googleapis.com/auth/drive.readonly'
        ],
    });
    const authClient = await auth.getClient();
    google.options({ auth: authClient });

    return {
        sheets: google.sheets({ version: 'v4', auth: authClient }),
        drive: google.drive({ version: 'v3', auth: authClient })
    };
}

// Function to read data from Google Sheets
async function readSheetData(sheets, sheetID, range) {
	spreadsheetId = sheetID;
	try {
		const response = await sheets.spreadsheets.values.get({
			spreadsheetId,
			range,
		});
		return response.data.values;
	} catch (error) {
        console.error('Error occurred while reading data from the GSheet:', error);
        throw error;
    }
}

// Function to read multiple ranges at once
async function readMultipleRanges(sheets, sheetID, ranges) {
    try {
        const response = await sheets.spreadsheets.values.batchGet({
            spreadsheetId: sheetID,
            ranges: ranges,
        });
        return response.data.valueRanges.map(range => range.values);
    } catch (error) {
        console.error('Error occurred while reading data from the GSheet:', error);
        throw error;
    }
}

// Check GSheet setup
async function checkSetup(sheets, sheetID) {
    let namedRangesValues = {};
    const rangeNames = ['metadata_range', 'data_range', 'table_name', 'drop_table','first_ingestion','ingestion_type','frequency_unit','frequency_value'];
	
    validSetup = false;
	spreadsheetId = sheetID;
	try {
        const response = await sheets.spreadsheets.get({
            spreadsheetId,
            fields: 'namedRanges'
        });

		const namedRanges = response.data.namedRanges || [];
        const missingRanges = [];
		const foundRanges = [];
		
        for (const rangeName of rangeNames) {
			const foundRange = namedRanges.find(namedRange => namedRange.name === rangeName || namedRange.name === `'Setup'!${rangeName}`);
			
			if (foundRange) {
				foundRanges.push(foundRange.name);
            } else {
				missingRanges.push(rangeName);
            }
        }
		
		if (missingRanges.length === 0) {
			namedRangesValues = await readMultipleRanges(sheets, sheetID, foundRanges);
			Object.keys(namedRangesValues).forEach(key => {
				// Assuming that each property is an array of arrays and we need the first element of the inner array
				if (Array.isArray(namedRangesValues[key]) && Array.isArray(namedRangesValues[key][0])) {
					namedRangesValues[key] = namedRangesValues[key][0][0];
				}
			});
			var setup = {};
			rangeNames.forEach((name, index) => {
				setup[name] = namedRangesValues[index];
			});
			const hasEmptyValue = namedRangesValues.some(element => element === '' || element === null || element === undefined);
			if (hasEmptyValue) {
				console.log('Setup is NOT OK. Please check parameters in the setup sheet.');
			} else {
				validSetup = true;
				console.log(setup);
			}
        } else {
            console.log('Setup is NOT OK. Missing named ranges: ' + missingRanges.join(', '));
        }
		return { setup, validSetup };
		
    } catch (error) {
        console.error('Error occurred while checking for named ranges:', error);
        throw error;
    }
}

// Function to test if a value is formatted as numeric
function isNumeric(value) {
    return /^-?\d+(,\d+)*(\.\d+)?$/.test(value);
}

// Function for stripping a numeric value to raw format
function cleanNumericValue(value) {
    return value.replace(/,/g, '');
}

// Function to insert a log entry
async function insertLogEntry(processID, sheetID, le_desc, le_cat, err_msg) {
	const processIDiso = toLocalISOString(processID);
	const client = await pool.connect();
    const le_timestamp = toLocalISOString(new Date());
	if (err_msg == undefined) {
		err_msg = "";
	}
	try {
        await client.query('BEGIN');
		query = "insert into " + log_table_name + " (gsheet_id, process_id, log_entry_desc, log_entry_timestamp, log_entry_category, err_message) values ('" + sheetID + "', '" + processIDiso + "', '" + le_desc + "', '" +  le_timestamp + "', '" + le_cat + "', '" + err_msg + "');"; 
		console.log(query);
		await client.query(query);
        await client.query('COMMIT');
    } catch (e) {
        await client.query('ROLLBACK');
        throw e;
    } finally {
        client.release();
    }
}

// Function to create or recreate the table container on PGSQL
async function createTableContainer(processID, sheetID, table_name, metadata) {
    //const columnDefinitions = metadata.map(column => `${column[0]} ${column[1]}`).join(', '); // old code. doesn't work for column names with spaces in them.
	const columnDefinitions = metadata.map(column => `"${column[0]}" ${column[1]}`).join(', ');
	console.log('Fields structure: ' + columnDefinitions);
	const client = await pool.connect();
    try {
        await client.query('BEGIN');
		query = 'drop table if exists '  + table_name + ' cascade;'
		console.log(query);
		await client.query(query);
		query = `CREATE TABLE ${table_name} (${columnDefinitions});`;
		await client.query(query);
		console.log(query);
        await client.query('COMMIT');
		await insertLogEntry(processID, sheetID, 'Table ' + table_name + ' was successgully created / recreated', 'info', undefined);
    } catch (e) {
        await client.query('ROLLBACK');
        await insertLogEntry(processID, sheetID, 'Error occured during creation / recreation of table ' + table_name, 'err', e);
		throw e;
    } finally {
        client.release();
    }
}

// Function to insert data into PostgreSQL
async function insertDataToPostgres(processID, sheetID, data, formattedColumnNames, formattedValues, table_name) {
    const client = await pool.connect();
	var itter = 0;
    try {
        await client.query('BEGIN');
        // Truncate the table before inserting new data
        const truncateQuery = `TRUNCATE TABLE ${table_name};`;
        await client.query(truncateQuery);
        console.log(truncateQuery);

        // Process data in batches
        for (let i = 0; i < data.length; i += batchSize) {
            itter = itter + 1;
			const batch = data.slice(i, i + batchSize);
            const values = [];
            const placeholders = batch.map((row, rowIndex) => {
                row.forEach((value, columnIndex) => {
                    if (columnIndex === 1 && isNumeric(value)) {
                        value = cleanNumericValue(value);
                    }
                    values.push(value);
                });
                return `(${new Array(row.length).fill().map((_, index) => `$${rowIndex * row.length + index + 1}`).join(', ')})`;
            }).join(', ');

            const query = `INSERT INTO ${table_name} ${formattedColumnNames} VALUES ${placeholders}`;
            await client.query(query, values);
        }

        await client.query('COMMIT');
        await insertLogEntry(processID, sheetID, 'Table ' + table_name + ' was successfully repopulated. (' + itter + ' itterations of ' + batchSize + ' records)', 'info', undefined);
    } catch (e) {
        await client.query('ROLLBACK');
        await insertLogEntry(processID, sheetID, 'Table ' + table_name + ' ingestion failed', 'err', e);
        throw e;
    } finally {
        client.release();
    }
}

// Run the ingestion cycle
async function ingestCycle(sheets, setup, processID, sheetID) {
    try {
		const data = await readSheetData(sheets, sheetID, setup.data_range);
		const metadata = await readSheetData(sheets, sheetID, setup.metadata_range);
		schema_name = 'raw_gsheets';
		var table_name = schema_name + '.' + setup.table_name;
		if (setup.drop_table == 'TRUE') {
			// Create / recreate the target table container
			await createTableContainer(processID, sheetID, table_name, metadata);
		}

		// Fields list
		const columnNames = metadata.map(column => `"${column[0]}"`).join(', ');
		const formattedColumnNames = `(${columnNames})`;
		// Values
		const placeholders = metadata.map((_, index) => `$${index + 1}`).join(', ');
		const formattedValues = `VALUES (${placeholders})`;
		// Truncate table and reingest the records
		await insertDataToPostgres(processID, sheetID, data, formattedColumnNames, formattedValues, table_name);
    } catch (error) {
        console.error('Error occurred:', error);
    }
}

// Check if it's time to run an ingestion for a specific GSheet
function shouldTriggerIngestion(setup, processID) {
    //console.log(setup.ingestion_type);
	if (setup.ingestion_type === 'Manual') {
        console.log('Ingestion set to manual. Skipping file.');
        return false;
    }

    const firstIngestionDate = new Date(setup.first_ingestion);
    const frequencyValue = parseInt(setup.frequency_value, 10);
    const minutesPerUnit = {
        'Minute': 1,
        'Hour': 60,
        'Day': 1440,
        'Week': 10080,
        // 'Month' calculation would be more complex as it varies
    };

    // Special case for Monthly frequency
    if (setup.frequency_unit === 'Month') {
        return processID.getDate() === firstIngestionDate.getDate() &&
               processID.getHours() === firstIngestionDate.getHours() &&
               processID.getMinutes() === firstIngestionDate.getMinutes();
    } else {
        // Calculate time difference in minutes
        const timeDifference = Math.floor((processID - firstIngestionDate) / (1000 * 60)); // Convert to minutes
        const unitMinutes = minutesPerUnit[setup.frequency_unit] * frequencyValue;

        // Check if the time difference is a multiple of the frequency
        return timeDifference >= 0 && (timeDifference % unitMinutes === 0);
    }
}

// Return current timestamp, rounded to the nearest minute.
function getCurrentTimeRoundedToMinute() {
    let now = new Date();

    // Set seconds and milliseconds to zero to round down to the nearest minute
    now.setSeconds(0, 0); // 0 seconds, 0 milliseconds

    return now;
}

// Get shared GSheets list
async function listGoogleSheets(drive) {
	const response = await drive.files.list({
		q: "mimeType='application/vnd.google-apps.spreadsheet'",
		corpora: 'drive', // Search within a specific drive
		driveId: 'drive_id_goes_here', // Specify the shared drive ID
		includeItemsFromAllDrives: true, // Necessary to include shared drive items
		supportsAllDrives: true, // Necessary to include shared drive items
		fields: 'files(id, name)',
	});
    const sheetsList = response.data.files;
	return sheetsList.map(sheet => ({ id: sheet.id, name: sheet.name }));
}

// Date formatting
function toLocalISOString(date) {
    const offset = date.getTimezoneOffset() * 60000;
    const localISOTime = (new Date(date - offset)).toISOString().slice(0, -1);
    return localISOTime;
}

// Main function to execute the process
async function main() {
    const { sheets, drive } = await authorizeGoogleServices();
	
	var counter = 0;
	const max_cycles = -1; // maximum number of itterations. set to -1 for endless loop.
	if (max_cycles == -1) {
		perpetual = 1;
	}
	else {
		perpetual = 0;
	}
	while (counter <= max_cycles || perpetual == 1) { // Endless loop, or limited to [max_cycles] itterations
        counter = counter + 1;
		var processID = getCurrentTimeRoundedToMinute();
		console.log('Cycle timestamp (process ID): ' + processID);
		try {
            const sheetsList = await listGoogleSheets(drive);
			console.log("Sheets found: ");
			console.log(sheetsList);
            for (const sheet of sheetsList) {
                console.log(`Checking sheet: ${sheet.name} (ID: ${sheet.id})`);
                var { setup, validSetup } = await checkSetup(sheets, sheet.id); // Assuming checkSetup now also takes the sheet ID
				if (validSetup) {
					if (shouldTriggerIngestion(setup, processID)) {
						console.log('Triggering ingestion...');
						await ingestCycle(sheets, setup, processID, sheet.id);
					} else {
						console.log('Not time for ingestion yet.');
					}
                    //await ingestCycle(setup, sheets);
                } else {
                    console.log(`Sheet ${sheet.name} is not valid for ingestion.`);
                }
            }
        } catch (error) {
            console.error('Error occurred:', error);
        }

        console.log('Waiting for next cycle...');
        await new Promise(resolve => setTimeout(resolve, 60 * 1000)); // Wait for 1 minute
    }
}

main();
