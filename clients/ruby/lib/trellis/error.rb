# frozen_string_literal: true

module Trellis
  # An error from Trellis: the engine's message and a stable #code, and
  # nothing else (ADR-0010 decision 4). Every error the binding raises is one
  # of the subclasses below, so `rescue Trellis::Error` catches them all, and
  # `rescue Trellis::NotFoundError` just one kind.
  #
  # The engine's error codes are an open set, so a code this version of the
  # binding doesn't know arrives as an UnknownError, with the engine's code
  # named in the message, rather than crashing.
  class Error < StandardError
    # The code this class stands for, as a symbol; see #code.
    CODE = nil

    # One of :parse, :validation, :connectivity, :conflict, :not_found,
    # :internal, :timeout, or :unknown for a code newer than this binding.
    def code
      self.class::CODE
    end

    # The exception for the native extension's `(code, message)` pair.
    # Not part of the public API.
    def self.from_native(code, message)
      klass = BY_CODE[code]
      return klass.new(message) if klass

      UnknownError.new("(error code #{code.inspect}) #{message}")
    end
  end

  # The statement text isn't valid grammar.
  class ParseError < Error
    CODE = :parse
  end

  # Well-formed but rejected: a bad option, an unknown column, a type
  # mismatch, a call on a handle that has been shut down.
  class ValidationError < Error
    CODE = :validation
  end

  # The database couldn't be reached, or the connection dropped.
  class ConnectivityError < Error
    CODE = :connectivity
  end

  # Clashes with something that already exists.
  class ConflictError < Error
    CODE = :conflict
  end

  # Names something that doesn't exist.
  class NotFoundError < Error
    CODE = :not_found
  end

  # A Trellis bug or an unexpected database failure.
  class InternalError < Error
    CODE = :internal
  end

  # A bounded wait ran out of time. Mapped ahead of the engine: #586 gives
  # Trellis this code, and until it lands nothing raises it.
  class TimeoutError < Error
    CODE = :timeout
  end

  # An error code newer than this binding. The message names the code.
  class UnknownError < Error
    CODE = :unknown
  end

  # A call on a handle this process inherited through `fork` rather than
  # connected itself. Rust threads don't cross `fork`, so the handle has
  # nothing left to answer it: every call raises this instead of hanging.
  # Call Trellis.connect in the forked process (Puma's on_worker_boot,
  # Passenger's starting_worker_process). A ValidationError, like any other
  # call on a handle that can't serve it.
  #
  # Also Trellis.connect's error in a process forked while its parent had a
  # handle running, which may have inherited a lock that handle's threads
  # held (issue #600): shut down before forking (Puma's before_fork).
  class ForkedHandleError < ValidationError
  end

  class Error
    # The engine's codes, written out rather than derived, so adding one is a
    # deliberate change here. test/error_test.rb fails when the engine
    # reports a code this map lacks.
    BY_CODE = {
      "parse" => ParseError,
      "validation" => ValidationError,
      "connectivity" => ConnectivityError,
      "conflict" => ConflictError,
      "not_found" => NotFoundError,
      "internal" => InternalError,
      "timeout" => TimeoutError
    }.freeze
  end
end
