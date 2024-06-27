package com.ovhcloud.edc.extension.s3.validation;

import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.validator.spi.ValidationResult;
import org.eclipse.edc.validator.spi.Validator;
import java.util.Objects;
import java.util.stream.Stream;

import static org.eclipse.edc.validator.spi.Violation.violation;

/**
 * Base class for S3 data address validators.
 */
public abstract class S3DataAddressValidator implements Validator<DataAddress> {

  /**
   * Validate that the given fields are valid (not null or empty). If a field is invalid, a
   * violation is returned and the validation fails.
   *
   * @param dataAddress the data address to validate
   * @param fields      the list of fields to check.
   * @return the validation result. If the validation is successful, the result is a success. If the
   * validation fails, the result is a failure with the list of violations.
   */
  public ValidationResult validate(DataAddress dataAddress, String... fields) {
    var violations = Stream.of(fields)
        .map(it -> {
          var value = dataAddress.getStringProperty(it);
          if (value == null || value.isBlank()) {
            return violation("The field " + it + " is required", it, value);
          }
          return null;
        })
        .filter(Objects::nonNull)
        .toList();

    if (violations.isEmpty()) {
      return ValidationResult.success();
    }

    return ValidationResult.failure(violations);
  }
}
