# -*- coding: utf-8 -*-

# This file is part of the Ingram Micro Cloud Blue Connect SDK.
# Copyright (c) 2019 Ingram Micro. All Rights Reserved.

""" This module provides the :py:class:`.MigrationHandler` class, which helps migrating data
from a legacy service into Connect.

We can use an instance of this class into our request processor, like this: ::

    class PurchaseFlow(Flow):
        def __init__(self):
            super().__init__(lambda request: request.type == 'purchase')
            self.step('Checking if migration is need', PurchaseFlow._need_migration)
            self.step('Provision real purchase order', PurchaseFlow._purchase)
            self.step('Approving request', PurchaseFlow._approve_request)



            def _need_migration(self):
                self.getRequest().migration_handler = MigrationHandler({
                'email': lambda data, request_id: data['teamAdminEmail'].upper(),
                'team_id': lambda data, request_id: data['teamId'].upper(),
                'team_name': lambda data, request_id: data['teamName'].upper(),
                'num_licensed_users': lambda data, request_id: int(data['licNumber']) * 10
                })

                if self.getRequest().needsMigration():
                    req = self.migration_handler.migrate(self.getRequest())
                    self.getRequest().asset.params = req.asset.params
                    self._approve_request()

"""

import copy
import json

import six
from typing import List

from connect import Env
from connect.models import AssetRequest


class MigrationAbortError(Exception):
    pass


class MigrationParamError(Exception):
    pass


class MigrationHandler(object):
    """ This class helps migrating data from a legacy service into Connect.

    :param dict[str,callable] transformations: Contains the param id as keys, and the function that
      produces the value of the parameter. This function will receive two arguments:

      - **transform_data** (dict[str, Any]): Contains the entire transformation information as a
        JSON object, parsed from the param with ``migration_key`` id.
      - **request_id** (str): The id of the request being processed.
    :param str migration_key: The name of the Connect parameter that stores the legacy data
      in json format. Default value is ``migration_info``.
    :param bool serialize: If ``True``, it will automatically serialize any non-string value
      in the migration data on direct assignation flow. Default value is ``False``.
    """

    def __init__(self, transformations=None, migration_key='migration_info',
                 serialize=False):
        self._transformations = transformations or {}
        self._migration_key = migration_key
        self._serialize = serialize

    @property
    def transformations(self):
        """
        :return: The transformations defined for the handler.
        :rtype: dict[str, callable]
        """
        return self._transformations

    @property
    def migration_key(self):
        """
        :return: The id of the parameter that contains the migration data.
        :rtype: str
        """
        return self._migration_key

    @property
    def serialize(self):
        """
        :return: Whether the migration data is serialized automatically when it does not have
          a transformation function and the data is not a string.
        :rtype: bool
        """
        return self._serialize

    def migrate(self, request):
        """ Call this function to perform migration of one request.

        :param AssetRequest request: The request to migrate.
        :return: A new request object with the parameter values updated.
        :rtype: AssetRequest
        :raises MigrationParamError: Raised if the value for a parameter is not a string.
        """
        if request.needsMigration(self.migration_key):
            Env.getLogger().info(
                '[MIGRATION::{}] Running migration operations for request {}'
                .format(request.id, request.id))
            request_copy = copy.deepcopy(request)

            raw_data = request.asset.get_param_by_id(self.migration_key).value
            Env.getLogger().debug('[MIGRATION::{}] Migration data `{}`: {}'
                                  .format(request.id, self.migration_key,
                                          raw_data))

            try:
                try:
                    parsed_data = json.loads(raw_data)
                except ValueError as ex:
                    raise MigrationAbortError(str(ex))
                Env.getLogger().debug(
                    '[MIGRATION::{}] Migration data `{}` parsed correctly'
                    .format(request.id, self.migration_key))

                # These will keep track of processing status
                processed_params = []
                succeeded_params = []
                failed_params = []
                skipped_params = []

                # Exclude param for migration_info from process list
                params = [param for param in request_copy.asset.params.toArray()
                          if param.id != self.migration_key]

                for param in params:
                    # Try to process the param and report success or failure
                    try:
                        if param.id in self.transformations:
                            # Transformation is defined, so apply it
                            Env.getLogger().info(
                                '[MIGRATION::{}] Running transformation for parameter {}'
                                .format(request.id, param.id))
                            param.value = self.transformations[param.id](
                                parsed_data, request.id)
                            succeeded_params.append(param.id)
                        elif param.id in parsed_data:
                            # Parsed data contains the key, so assign it
                            if not isinstance(parsed_data[param.id],
                                              six.string_types):
                                if self.serialize:
                                    parsed_data[param.id] = json.dumps(
                                        parsed_data[param.id])
                                else:
                                    type_name = type(
                                        parsed_data[param.id]).__name__
                                    raise MigrationParamError(
                                        'Parameter {} type must be str, but {} was given'
                                            .format(param.id, type_name))
                            param.value = parsed_data[param.id]
                            succeeded_params.append(param.id)
                        else:
                            skipped_params.append(param.id)
                    except MigrationParamError as ex:
                        Env.getLogger().error(
                            '[MIGRATION::{}] {}'.format(request.id, ex))
                        failed_params.append(param.id)

                    # Report processed param
                    processed_params.append(param.id)

                Env.getLogger().info(
                    '[MIGRATION::{}] {} processed, {} succeeded{}, {} failed{}, '
                    '{} skipped{}.'
                        .format(
                        request.id,
                        len(processed_params),
                        len(succeeded_params),
                        self._format_params(succeeded_params),
                        len(failed_params),
                        self._format_params(failed_params),
                        len(skipped_params),
                        self._format_params(skipped_params)))

                # Raise abort if any params failed
                if failed_params:
                    raise MigrationAbortError(
                        'Processing of parameters {} failed, unable to complete migration.'
                            .format(', '.join(failed_params)))
            except MigrationAbortError as ex:
                Env.getLogger().error(
                    '[MIGRATION::{}] {}'.format(request.id, ex))
                raise

            return request_copy
        else:
            Env.getLogger().info(
                '[MIGRATION::{}] AssetRequest does not need migration.'
                .format(request.id))
            return request

    @staticmethod
    def _format_params(params):
        # type: (List[str]) -> str
        return ' (' + ', '.join(params) + ')' if len(params) > 0 else ''
