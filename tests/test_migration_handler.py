# -*- coding: utf-8 -*-

# This file is part of the Ingram Micro Cloud Blue Connect SDK.
# Copyright (c) 2019 Ingram Micro. All Rights Reserved.

import os
import six
import pytest
from mock import patch, Mock, call
from typing import Dict, Optional

import connect_migration
from connect_migration import MigrationAbortError
from connect.models import Model, AssetRequest
from connect import Collection


def _load_str(filename):
    # type: (str) -> Optional[str]
    try:
        filename = os.path.join(
            os.path.dirname(__file__),
            'data',
            filename
        )
        with open(filename) as file_handle:
            return file_handle.read()
    except IOError:
        return None


def test_properties():
    # type: () -> None
    handler = connect_migration.MigrationHandler()
    assert isinstance(handler, connect_migration.MigrationHandler)
    assert isinstance(handler.transformations, dict)
    assert isinstance(handler.migration_key, str)
    assert isinstance(handler.serialize, bool)
    assert len(handler.transformations) == 0
    assert handler.migration_key == 'migration_info'
    assert not handler.serialize


def test_needsMigration():
    # type: () -> None
    handler = connect_migration.MigrationHandler()

    # No migration needed
    response_no_migration = _load_str('response.json')
    requests_no_migration = Model.parseArray(AssetRequest,
                                             response_no_migration)
    assert isinstance(requests_no_migration, Collection)
    assert requests_no_migration.length() == 1
    assert isinstance(requests_no_migration.get(0), AssetRequest)
    assert not requests_no_migration.get(0).needsMigration(
        handler.migration_key)

    # Migration needed
    response_migration = _load_str('request.migrate.valid.json')
    request = Model.parse(AssetRequest, response_migration)
    assert isinstance(request, AssetRequest)
    assert request.needsMigration(handler.migration_key)


@patch('connect.logger.Logger.info')
def test_no_migration(info_mock):
    # type: (Mock) -> None
    response = _load_str('response.json')
    requests = Model.parseArray(AssetRequest, response)
    assert isinstance(requests, Collection)
    assert requests.length() == 1

    handler = connect_migration.MigrationHandler()
    request = handler.migrate(requests.get(0))
    info_mock.assert_called_once_with('[MIGRATION::PR-5852-1608-0000] '
                                      'AssetRequest does not need migration.')
    assert request == requests.get(0)


@patch('connect.logger.Logger.info')
@patch('connect.logger.Logger.debug')
def test_migration_skip_all(debug_mock, info_mock):
    # type: (Mock, Mock) -> None
    response = _load_str('request.migrate.valid.json')
    request = Model.parse(AssetRequest, response)

    handler = connect_migration.MigrationHandler()
    request_out = handler.migrate(request)

    assert info_mock.call_count == 2
    info_mock.assert_has_calls([
        call(
            '[MIGRATION::PR-7001-1234-5678] Running migration operations for request '
            'PR-7001-1234-5678'),
        call(
            '[MIGRATION::PR-7001-1234-5678] 5 processed, 0 succeeded, 0 failed, 5 skipped '
            '(email, num_licensed_users, reseller_id, team_id, team_name).')
    ])

    assert debug_mock.call_count == 2
    debug_mock.assert_has_calls([
        call('[MIGRATION::PR-7001-1234-5678] Migration data `migration_info`: {'
             '"teamAdminEmail":"example.migration@mailinator.com",'
             '"teamId":"dbtid:AADaQq_w53nMDQbIPM_X123456PuzpcM2BI",'
             '"resellerId":["3ONEYO1234"],'
             '"teamName":"Migration Team",'
             '"licNumber":"10"}'),
        call(
            '[MIGRATION::PR-7001-1234-5678] Migration data `migration_info` parsed correctly')
    ])
    assert request_out.toString() == request.toString()
    assert request_out.id == 'PR-7001-1234-5678'
    assert request_out.asset.id == 'AS-146-621-424-3'
    assert len(request_out.asset.params.toArray()) == 6
    for i, _ in enumerate(request_out.asset.params.toArray()):
        assert request.asset.params.get(i).id == request_out.asset.params.get(
            i).id
        assert request.asset.params.get(
            i).value == request_out.asset.params.get(i).value


@patch('connect.logger.Logger.error')
@patch('connect.logger.Logger.debug')
@patch('connect.logger.Logger.info')
def test_migration_wrong_info(info_mock, debug_mock, error_mock):
    # type: (Mock, Mock, Mock) -> None
    response = _load_str('request.migrate.invalid.json')
    request = Model.parse(AssetRequest, response)

    handler = connect_migration.MigrationHandler()
    with pytest.raises(MigrationAbortError):
        handler.migrate(request)

    assert info_mock.call_count == 1
    info_mock.assert_called_with(
        '[MIGRATION::PR-7001-1234-5678] Running migration operations '
        'for request PR-7001-1234-5678')

    assert debug_mock.call_count == 1
    debug_mock.assert_called_with(
        '[MIGRATION::PR-7001-1234-5678] Migration data `migration_info`: '
        '"teamAdminEmail":"example.migration@mailinator.com",'
        '"teamId":"dbtid:AADaQq_w53nMDQbIPM_X123456PuzpcM2BI",'
        '"resellerId":["3ONEYO1234"],'
        '"teamName":"Migration Team",'
        '"licNumber":"10"}')

    assert error_mock.call_count == 1
    # The following assertion fails on macOS, so it has been disabled for now
    # error_mock.assert_called_with('[MIGRATION::PR-7001-1234-5678] Extra data: '
    #                              'line 1 column 17 - line 1 column 179 (char 16 - 178)')


@patch('connect.logger.Logger.info')
@patch('connect.logger.Logger.debug')
def test_migration_direct(debug_mock, info_mock):
    # type: (Mock, Mock) -> None
    response = _load_str('request.migrate.direct.success.json')
    request = Model.parse(AssetRequest, response)

    handler = connect_migration.MigrationHandler()
    request_out = handler.migrate(request)

    assert request_out != request
    assert request_out.id == 'PR-7001-1234-5678'
    assert request_out.asset.id == 'AS-146-621-424-3'
    assert request_out.asset.params.length() == 6
    assert request_out.asset.get_param_by_id(
        'email').value == 'example.migration@mailinator.com'
    assert request_out.asset.get_param_by_id('num_licensed_users').value == '10'
    assert request_out.asset.get_param_by_id('reseller_id').value == ''
    assert request_out.asset.get_param_by_id('team_id').value == 'dbtid:AADaQq_' \
                                                                 'w53nMDQbIPM_X123456PuzpcM2BI'
    assert request_out.asset.get_param_by_id(
        'team_name').value == 'Migration Team'

    assert info_mock.call_count == 2
    info_mock.assert_has_calls([
        call(
            '[MIGRATION::PR-7001-1234-5678] Running migration operations for request '
            'PR-7001-1234-5678'),
        call('[MIGRATION::PR-7001-1234-5678] 5 processed, 4 succeeded '
             '(email, num_licensed_users, team_id, team_name), 0 failed, 1 skipped (reseller_id).')
    ])

    assert debug_mock.call_count == 2
    debug_mock.assert_has_calls([
        call('[MIGRATION::PR-7001-1234-5678] Migration data `migration_info`: {'
             '"email":"example.migration@mailinator.com",'
             '"team_id":"dbtid:AADaQq_w53nMDQbIPM_X123456PuzpcM2BI",'
             '"team_name":"Migration Team",'
             '"num_licensed_users":"10"}'),
        call(
            '[MIGRATION::PR-7001-1234-5678] Migration data `migration_info` parsed correctly')
    ])


@patch('connect.logger.Logger.info')
@patch('connect.logger.Logger.debug')
def test_migration_direct_serialize(debug_mock, info_mock):
    # type: (Mock, Mock) -> None
    response = _load_str('request.migrate.direct.notserialized.json')
    request = Model.parse(AssetRequest, response)

    handler = connect_migration.MigrationHandler(serialize=True)
    request_out = handler.migrate(request)

    assert request_out != request
    assert request_out.id == 'PR-7001-1234-5678'
    assert request_out.asset.id == 'AS-146-621-424-3'
    assert request_out.asset.params.length() == 6
    assert request_out.asset.get_param_by_id(
        'email').value == 'example.migration@mailinator.com'
    team_name = request_out.asset.get_param_by_id('team_name').value
    assert isinstance(team_name, six.string_types)
    assert team_name == '["Some name"]'

    assert info_mock.call_count == 2
    info_mock.assert_has_calls([
        call(
            '[MIGRATION::PR-7001-1234-5678] Running migration operations for request '
            'PR-7001-1234-5678'),
        call('[MIGRATION::PR-7001-1234-5678] 5 processed, 2 succeeded '
             '(email, team_name), 0 failed, 3 skipped (num_licensed_users, reseller_id, team_id).')
    ])

    assert debug_mock.call_count == 2
    debug_mock.assert_has_calls([
        call('[MIGRATION::PR-7001-1234-5678] Migration data `migration_info`: {'
             '"email":"example.migration@mailinator.com",'
             '"team_name":["Some name"]}'),
        call(
            '[MIGRATION::PR-7001-1234-5678] Migration data `migration_info` parsed correctly')
    ])


@patch('connect.logger.Logger.error')
@patch('connect.logger.Logger.debug')
@patch('connect.logger.Logger.info')
def test_migration_direct_no_serialize(info_mock, debug_mock, error_mock):
    # type: (Mock, Mock, Mock) -> None
    response = _load_str('request.migrate.direct.notserialized.json')
    request = Model.parse(AssetRequest, response)

    handler = connect_migration.MigrationHandler()
    with pytest.raises(MigrationAbortError):
        handler.migrate(request)

    assert info_mock.call_count == 2
    info_mock.assert_has_calls([
        call('[MIGRATION::PR-7001-1234-5678] Running migration operations '
             'for request PR-7001-1234-5678'),
        call('[MIGRATION::PR-7001-1234-5678] 5 processed, 1 succeeded (email), '
             '1 failed (team_name), 3 skipped (num_licensed_users, reseller_id, team_id).')
    ])

    assert debug_mock.call_count == 2
    debug_mock.assert_has_calls([
        call('[MIGRATION::PR-7001-1234-5678] Migration data `migration_info`: {'
             '"email":"example.migration@mailinator.com",'
             '"team_name":["Some name"]}'),
        call(
            '[MIGRATION::PR-7001-1234-5678] Migration data `migration_info` parsed correctly')
    ])

    assert error_mock.call_count == 2
    error_mock.assert_has_calls([
        call(
            '[MIGRATION::PR-7001-1234-5678] Parameter team_name type must be str, '
            'but list was given'),
        call(
            '[MIGRATION::PR-7001-1234-5678] Processing of parameters team_name failed, '
            'unable to complete migration.')
    ])


@patch('connect.logger.Logger.info')
@patch('connect.logger.Logger.debug')
def test_migration_transform(debug_mock, info_mock):
    # type: (Mock, Mock) -> None
    response = _load_str('request.migrate.transformation.json')
    request = Model.parse(AssetRequest, response)

    handler = connect_migration.MigrationHandler({
        'email': lambda data, request_id: data['teamAdminEmail'].upper(),
        'team_id': lambda data, request_id: data['teamId'].upper(),
        'team_name': lambda data, request_id: data['teamName'].upper(),
        'num_licensed_users': lambda data, request_id: int(
            data['licNumber']) * 10
    })
    request_out = handler.migrate(request)

    assert request_out != request
    assert request_out.id == 'PR-7001-1234-5678'
    assert request_out.asset.id == 'AS-146-621-424-3'
    assert request_out.asset.params.length() == 6
    assert request_out.asset.get_param_by_id(
        'email').value == 'EXAMPLE.MIGRATION@MAILINATOR.COM'
    assert request_out.asset.get_param_by_id('num_licensed_users').value == 100
    assert request_out.asset.get_param_by_id('reseller_id').value == ''
    assert request_out.asset.get_param_by_id('team_id').value == 'DBTID:AADAQQ_' \
                                                                 'W53NMDQBIPM_X123456PUZPCM2BI'
    assert request_out.asset.get_param_by_id(
        'team_name').value == 'MIGRATION TEAM'

    assert info_mock.call_count == 6
    info_mock.assert_has_calls([
        call(
            '[MIGRATION::PR-7001-1234-5678] Running migration operations for request '
            'PR-7001-1234-5678'),
        call(
            '[MIGRATION::PR-7001-1234-5678] Running transformation for parameter email'),
        call(
            '[MIGRATION::PR-7001-1234-5678] Running transformation for parameter '
            'num_licensed_users'),
        call(
            '[MIGRATION::PR-7001-1234-5678] Running transformation for parameter team_id'),
        call(
            '[MIGRATION::PR-7001-1234-5678] Running transformation for parameter team_name'),
        call('[MIGRATION::PR-7001-1234-5678] 5 processed, 4 succeeded '
             '(email, num_licensed_users, team_id, team_name), 0 failed, 1 skipped (reseller_id).')
    ])

    assert debug_mock.call_count == 2
    debug_mock.assert_has_calls([
        call('[MIGRATION::PR-7001-1234-5678] Migration data `migration_info`: {'
             '"teamAdminEmail":"example.migration@mailinator.com",'
             '"teamId":"dbtid:AADaQq_w53nMDQbIPM_X123456PuzpcM2BI",'
             '"teamName":"Migration Team",'
             '"licNumber":"10"}'),
        call(
            '[MIGRATION::PR-7001-1234-5678] Migration data `migration_info` parsed correctly')
    ])


@patch('connect.logger.Logger.error')
@patch('connect.logger.Logger.debug')
@patch('connect.logger.Logger.info')
def test_migration_transform_manual_fail(info_mock, debug_mock, error_mock):
    # type: (Mock,Mock,Mock) -> None
    response = _load_str('request.migrate.transformation.json')
    request = Model.parse(AssetRequest, response)

    handler = connect_migration.MigrationHandler({
        'email': _raise_error
    })
    with pytest.raises(MigrationAbortError):
        handler.migrate(request)

    assert info_mock.call_count == 3
    info_mock.assert_has_calls([
        call(
            '[MIGRATION::PR-7001-1234-5678] Running migration operations for request PR-7001-1234-5678'),
        call(
            '[MIGRATION::PR-7001-1234-5678] Running transformation for parameter email'),
        call(
            '[MIGRATION::PR-7001-1234-5678] 5 processed, 0 succeeded, 1 failed (email), '
            '4 skipped (num_licensed_users, reseller_id, team_id, team_name).')
    ])

    assert debug_mock.call_count == 2
    debug_mock.assert_has_calls([
        call('[MIGRATION::PR-7001-1234-5678] Migration data `migration_info`: {'
             '"teamAdminEmail":"example.migration@mailinator.com",'
             '"teamId":"dbtid:AADaQq_w53nMDQbIPM_X123456PuzpcM2BI",'
             '"teamName":"Migration Team",'
             '"licNumber":"10"}'),
        call(
            '[MIGRATION::PR-7001-1234-5678] Migration data `migration_info` parsed correctly')
    ])

    assert error_mock.call_count == 2
    error_mock.assert_has_calls([
        call('[MIGRATION::PR-7001-1234-5678] Manual fail.'),
        call(
            '[MIGRATION::PR-7001-1234-5678] Processing of parameters email failed, '
            'unable to complete migration.')
    ])


def _raise_error(_, __):
    # type: (Dict[str, str], str) -> None
    raise connect_migration.MigrationParamError('Manual fail.')
