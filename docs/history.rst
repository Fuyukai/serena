.. _history:

Version History
===============

0.9.1 (2024-07-03)
==================

- Fix channel pools deadlocking everything on connection cancellation.

- Fix a bug in frameparsers not truncating the end.

0.9.0 (2024-01-25)
==================

- Fix all Pyright strict mode errors.
- Change channel objects to use regular dicts instead of ``**kwargs`` for connection-specific
  arguments.

  Some implementations (i.e. RabbitMQ) uses extensions in the format ``X-Argument``, which obviously
  isn't supported by Python.

0.8.1 (2023-11-25)
------------------

- Properly re-export names in ``__init__.py``.
- Remove the hacky ``logger.trace``. This also achieves 100% type completion.

0.8.0 (2023-11-22)
------------------

- Fully type and mark as ``py.typed``.
- Update to AnyIO 4.0.0 properly. This includes making some methods no longer raise ExceptionGroups
  due to hidden nurseries.

0.7.3 (2022-04-21)
------------------

- Remove ACK methods from :class:`.ChannelPool`. They don't really make sense.
- Add manual ``checkout`` method on :class:`.ChannelPool`.

0.7.2 (2022-02-03)
------------------

- Add the ``ChannelDelegate`` class.

0.7.1 (2022-01-29)
------------------

- Add the ``ChannelPool`` API.
- Fix a race condition with channel publishing.

0.7.0
-----

- Initial release.
