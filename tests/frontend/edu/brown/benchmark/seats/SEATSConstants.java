/***************************************************************************
 *  Copyright (C) 2011 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package edu.brown.benchmark.seats;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class SEATSConstants {
    
    // ----------------------------------------------------------------
    // STORED PROCEDURE EXECUTION FREQUENCIES (0% - 100%)
    // ----------------------------------------------------------------
    
    public static final int FREQUENCY_DELETE_RESERVATION        = 10;
    public static final int FREQUENCY_FIND_FLIGHTS              = 10;
    public static final int FREQUENCY_FIND_OPEN_SEATS           = 35;
    public static final int FREQUENCY_NEW_RESERVATION           = 20;
    public static final int FREQUENCY_UPDATE_CUSTOMER           = 10;
    public static final int FREQUENCY_UPDATE_RESERVATION        = 15;
    
//    public static final int FREQUENCY_DELETE_RESERVATION        = 0;
//    public static final int FREQUENCY_FIND_FLIGHTS              = 0;
//    public static final int FREQUENCY_FIND_OPEN_SEATS           = 100;
//    public static final int FREQUENCY_NEW_RESERVATION           = 0;
//    public static final int FREQUENCY_UPDATE_CUSTOMER           = 0;
//    public static final int FREQUENCY_UPDATE_RESERVATION        = 0;

    // ----------------------------------------------------------------
    // ERRORS
    // ----------------------------------------------------------------
    
    public enum ErrorType {
        INVALID_FLIGHT_ID,
        INVALID_CUSTOMER_ID,
        NO_MORE_SEATS,
        SEAT_ALREADY_RESERVED,
        CUSTOMER_ALREADY_HAS_SEAT,
        VALIDITY_ERROR,
        UNKNOWN;
        
        private final String errorCode;
        private final static Pattern p = Pattern.compile("^(USER ABORT:[\\s]+)?E([\\d]{4})");
        
        private ErrorType() {
            this.errorCode = String.format("E%04d", this.ordinal());
        }
        
        public static ErrorType getErrorType(String msg) {
            Matcher m = p.matcher(msg);
            if (m.find()) {
                int idx = Integer.parseInt(m.group(2));
                return ErrorType.values()[idx];
            }
            return (ErrorType.UNKNOWN);
        }
        @Override
        public String toString() {
            return this.errorCode;
        }
    }
    
    
    // ----------------------------------------------------------------
    // CONSTANTS
    // ----------------------------------------------------------------
    
    /** 
     * The different distances that we can look-up for nearby airports
     * This is similar to the customer selecting a dropdown when looking for flights 
     */
    public static final int DISTANCES[] = { 5 }; // , 10, 25, 50, 100 };
    
    /** The number of days in the past that we will generate flight information for */
    public static final int DAYS_PAST = 1;

    /** The number of days in the future that we will generate flight information for */
    public static final int DAYS_FUTURE = 365;
    
    /** Default number of customers in the database */
    public static final int NUM_CUSTOMERS = 1000000;

    /**
     * Average # of flights per day
     * NUM_FLIGHTS_PER_DAY = 15000
     * Source: http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time
     */
    public static final int MIN_FLIGHTS_PER_DAY = 11250;
    public static final int MAX_FLIGHTS_PER_DAY = 18750;
    
    /** Max Number of FREQUENT_FLYER records per CUSTOMER */ 
    public static final int MAX_FREQUENTFLYER_PER_CUSTOMER = 5;
    
    /**
     * Number of seats available per flight
     * If you change this then you must also change FindOpenSeats
     */
    public static final int NUM_SEATS_PER_FLIGHT = 150;
    
    /**
     * How many seats in the front of the plan are considered "first class"
     * These seats will have a higher price.
     */
    public static final int FIRST_CLASS_SEATS_OFFSET = 10;
    
    /**
     * The maximum number of days that we allow a customer to wait before needing
     * a reservation on a return to their original departure airport
     */
    public static final int MAX_RETURN_FLIGHT_DAYS = 14;   
    
    /** The rate in which a flight can travel between two airports (miles per hour) */
    public static final double FLIGHT_TRAVEL_RATE = 570.0; // Boeing 747
    
    public static final int MIN_RESERVATION_PRICE = 100;
    public static final int MAX_RESERVATION_PRICE = 1000;
    
    // ----------------------------------------------------------------
    // PROBABILITIES
    // ----------------------------------------------------------------
    
    /** Probability that a customer books a non-roundtrip flight (0% - 100%) */
    public static final int PROB_SINGLE_FLIGHT_RESERVATION = 10;
    
    /**
     * Probability that a customer will invoke DeleteReservation using the string
     * version of their Customer Id (0% - 100%)
     */
    public static final int PROB_DELETE_WITH_CUSTOMER_ID_STR = 10;

    /**
     * Probability that a customer will invoke DeleteReservation using the string
     * version of their FrequentFlyer Id (0% - 100%)
     */
    public static final int PROB_DELETE_WITH_FREQUENTFLYER_ID_STR = 10;
    
    /**
     * Probability that a customer will invoke UpdateCustomer using the string
     * version of their Customer Id (0% - 100%)
     */
    public static final int PROB_UPDATE_WITH_CUSTOMER_ID_STR = 20;
    
    /**
     * Probability that UpdateCustomer should update FrequentFlyer records (0% - 100%)
     */
    public static final int PROB_UPDATE_FREQUENT_FLYER = 25;
    
    /** Probability that is a seat is initially occupied (0% - 100%) */
    public static final int PROB_SEAT_OCCUPIED = 0; // 25;
    
    /** Probability that a new Reservation will be added to the DeleteReservation queue */
    public static final int PROB_DELETE_NEW_RESERVATION = 10;
    
    /** Probability that a new Reservation will be added to the UpdateReservation queue */
    public static final int PROB_UPDATE_NEW_RESERVATION = 25;

    /** Probability that a deleted Reservation will be requeued for another NewReservation call */
    public static final int PROB_REQUEUE_DELETED_RESERVATION = 90;
    
    /** Probability that FindFlights will use the distance search */
    public static final int PROB_FIND_FLIGHTS_NEARBY_AIRPORT = 25;
    
    /** Probability that FindFlights will perform a look up using a random pair of depart/arrive airports */
    public static final int PROB_FIND_FLIGHTS_RANDOM_AIRPORTS = 10;
    
    // ----------------------------------------------------------------
    // DATE CONSTANTS
    // ----------------------------------------------------------------
    
    /** Number of microseconds in a day */
    public static final long MICROSECONDS_PER_MINUTE = 60000000l;
    
    /** Number of microseconds in a day */
    public static final long MICROSECONDS_PER_DAY = 86400000000l; // 60sec * 60min * 24hr * 1,000,000 
    
    // ----------------------------------------------------------------
    // CACHE SIZES
    // ----------------------------------------------------------------
    
    /** The number of FlightIds we want to keep cached */
    public static final int CACHE_LIMIT_FLIGHT_IDS = 10000;
    
    public static final int CACHE_LIMIT_PENDING_INSERTS = 10000;
    public static final int CACHE_LIMIT_PENDING_UPDATES = 5000;
    public static final int CACHE_LIMIT_PENDING_DELETES = 5000;
    
    
    // ----------------------------------------------------------------
    // DATA SET INFORMATION
    // ----------------------------------------------------------------
    
    /**
     * Table Names
     */
    public static final String TABLENAME_COUNTRY = "COUNTRY";
    public static final String TABLENAME_AIRLINE = "AIRLINE";
    public static final String TABLENAME_CUSTOMER = "CUSTOMER";
    public static final String TABLENAME_FREQUENT_FLYER = "FREQUENT_FLYER";
    public static final String TABLENAME_AIRPORT = "AIRPORT";
    public static final String TABLENAME_AIRPORT_DISTANCE = "AIRPORT_DISTANCE";
    public static final String TABLENAME_FLIGHT = "FLIGHT";
    public static final String TABLENAME_RESERVATION = "RESERVATION";
    
    public static final String TABLENAME_CONFIG_PROFILE = "CONFIG_PROFILE";
    public static final String TABLENAME_CONFIG_HISTOGRAMS = "CONFIG_HISTOGRAMS";

    /**
     * Histogram Data Set Names
     */
    public static final String HISTOGRAM_FLIGHTS_PER_AIRPORT = "flights_per_airport";
    public static final String HISTOGRAM_FLIGHTS_PER_DEPART_TIMES = "flights_per_time";
    
    /** Tables that are loaded from data files */
    public static final String TABLE_DATA_FILES[] = {
        SEATSConstants.TABLENAME_COUNTRY,
        SEATSConstants.TABLENAME_AIRPORT,
        SEATSConstants.TABLENAME_AIRLINE,
    };
    
    /** Tables generated from random data */
    public static final String TABLE_SCALING[] = {
        SEATSConstants.TABLENAME_CUSTOMER,
        SEATSConstants.TABLENAME_FREQUENT_FLYER,
        SEATSConstants.TABLENAME_AIRPORT_DISTANCE,
        SEATSConstants.TABLENAME_FLIGHT,
        SEATSConstants.TABLENAME_RESERVATION,
    };
    
    /** Histograms generated from data files */
    public static final String HISTOGRAM_DATA_FILES[] = {
        SEATSConstants.HISTOGRAM_FLIGHTS_PER_AIRPORT,
        SEATSConstants.HISTOGRAM_FLIGHTS_PER_DEPART_TIMES,
    };

    /**
     * Tuple Code to Tuple Id Mapping
     * For some tables, we want to store a unique code that can be used to map
     * to the id of a tuple. Any table that has a foreign key reference to this table
     * will use the unique code in the input data tables instead of the id. Thus, we need
     * to keep a table of how to map these codes to the ids when loading.
     */
    public static final String CODE_TO_ID_COLUMNS[][] = {
        {TABLENAME_COUNTRY, "CO_CODE_3",    "CO_ID"},
        {TABLENAME_AIRPORT, "AP_CODE",      "AP_ID"},
        {TABLENAME_AIRLINE, "AL_IATA_CODE", "AL_ID"},
    };
}
