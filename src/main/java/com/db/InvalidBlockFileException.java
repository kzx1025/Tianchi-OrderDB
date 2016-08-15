package com.db;
/* Code for COMP261 Assignment
 * Name:
 * Usercode:
 * ID:
 */

import java.io.IOException;

/** InvalidBlockFileException   */

public class InvalidBlockFileException extends IOException{


    /** Construct a new InvalidBlockFileException object */
    public InvalidBlockFileException(int index){
	super("Invalid block at "+index);
    }



}
