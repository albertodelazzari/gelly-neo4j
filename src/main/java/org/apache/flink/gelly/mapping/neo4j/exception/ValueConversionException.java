/**
 * 
 */
package org.apache.flink.gelly.mapping.neo4j.exception;

/**
 * @author Alberto De Lazzari
 *
 */
public class ValueConversionException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 
	 */
	public ValueConversionException() {
		super();
	}

	/**
	 * @param message
	 */
	public ValueConversionException(String message) {
		super(message);
	}

	/**
	 * @param cause
	 */
	public ValueConversionException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public ValueConversionException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public ValueConversionException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}