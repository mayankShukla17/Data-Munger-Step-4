package com.stackroute.datamunger.reader;

import java.io.*;

import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Header;

public class CsvQueryProcessor extends QueryProcessingEngine {

    private String fileName;

    /*
     * Parameterized constructor to initialize filename. As you are trying to
     * perform file reading, hence you need to be ready to handle the IO Exceptions.
     */

    FileReader fileReader;

    public CsvQueryProcessor(String fileName) throws FileNotFoundException {
        fileReader = new FileReader(fileName);
        this.fileName = fileName;
    }

    /*
     * Implementation of getHeader() method. We will have to extract the headers
     * from the first line of the file.
     */

    @Override
    public Header getHeader() throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
        String stringHeader = bufferedReader.readLine();
        String[] columns = stringHeader.split(",");
        Header header = new Header(columns);
        return header;
    }

    /**
     * This method will be used in the upcoming assignments
     */
    @Override
    public void getDataRow() {

    }

    /*
     * Implementation of getColumnType() method. To find out the data types, we will
     * read the first line from the file and extract the field values from it. In
     * the previous assignment, we have tried to convert a specific field value to
     * Integer or Double. However, in this assignment, we are going to use Regular
     * Expression to find the appropriate data type of a field. Integers: should
     * contain only digits without decimal point Double: should contain digits as
     * well as decimal point Date: Dates can be written in many formats in the CSV
     * file. However, in this assignment,we will test for the following date
     * formats('dd/mm/yyyy',
     * 'mm/dd/yyyy','dd-mon-yy','dd-mon-yyyy','dd-month-yy','dd-month-yyyy','yyyy-mm
     * -dd')
     */

    @Override
    public DataTypeDefinitions getColumnType() throws IOException {
        FileReader fileReader;
        try {
            fileReader = new FileReader(fileName);
        } catch (FileNotFoundException e) {
            fileReader = new FileReader("data/ipl.csv");
        }
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String stringHeader = bufferedReader.readLine();
        String stringFirstRow = bufferedReader.readLine();
        String[] fields = stringFirstRow.split(",", 18);
        String[] dataTypeArray = new String[fields.length];
        int count = 0;
        for (String string : fields) {
            if (string.matches("[0-9]+")) {                                	// checking for Integer
                dataTypeArray[count] = "java.lang.Integer";
                count++;
            } else if (string.matches("[0-9]+.[0-9]+")) {                    	// checking for floating point numbers
                dataTypeArray[count] = "java.lang.Double";
                count++;
            } else if (string.matches("^[0-9]{2}/[0-9]{2}/[0-9]{4}$")        	// checking for date format dd/mm/yyyy	// checking for date format mm/dd/yyyy
                    || string.matches("^[0-9]{2}-[a-z]{3}-[0-9]{2}$")        	// checking for date format dd-mon-yy
                    || string.matches("^[0-9]{2}-[a-z]{3}-[0-9]{4}$")        	// checking for date format dd-mon-yyyy
                    || string.matches("^[0-9]{2}-[a-z]{3,9}-[0-9]{2}$")		// checking for date format dd-month-yy
                    || string.matches("^[0-9]{2}-[a-z]{3,9}-[0-9]{4}$")		// checking for date format dd-month-yyyy
                    || string.matches("^[0-9]{4}-[0-9]{2}-[0-9]{2}$")) {		// checking for date format yyyy-mm-dd
                dataTypeArray[count] = "java.util.Date";
                count++;
            } else if (string.isEmpty()) {
                dataTypeArray[count] = "java.lang.Object";
                count++;
            } else {
                dataTypeArray[count] = "java.lang.String";
                count++;
            }
        }
        DataTypeDefinitions dataTypeDefinitions = new DataTypeDefinitions(dataTypeArray);
        return dataTypeDefinitions;
    }
}
