.. _history:

Version History
===============

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
