import copy
import unittest
from unittest.mock import Mock

from market_scheduler import AssetSchedule


class HourlyScheduleTest(unittest.TestCase):
    assets = ('BTC', 'QQQ', 'VOO')

    @staticmethod
    def identity(asset, now):
        prefix = '' if asset == 'BTC' else asset.lower() + ':'
        return f'{prefix}hour:{int(now) // 3600 * 3600}'

    def callbacks(self, asset, send):
        return dict(send=send, pump=Mock(), expire=Mock(), save=Mock(),
                    report_identity=lambda now: self.identity(asset, now))

    def test_all_assets_use_hour_boundaries_despite_polling_delay(self):
        for asset in self.assets:
            with self.subTest(asset=asset):
                schedule = AssetSchedule(asset, None, 100)
                send = Mock(return_value='sent')
                callbacks = self.callbacks(asset, send)
                for now in (100, 3599, 3600.25, 3650, 7199, 7200.5, 7250):
                    schedule.tick(now, session={'open': True}, **callbacks)
                self.assertEqual([self.identity(asset, hour) for hour in (3600, 7200)],
                                 [call.kwargs['report_id'] for call in send.call_args_list])

    def test_repeated_disconnects_do_not_trigger_extra_reports(self):
        for asset in self.assets:
            with self.subTest(asset=asset):
                schedule = AssetSchedule(asset, None, 100)
                send = Mock(return_value='sent')
                callbacks = self.callbacks(asset, send)
                schedule.tick(3600.25, session={'open': True}, **callbacks)
                for now in (3700, 3900, 4200):
                    schedule.tick(now, session={'open': False}, **callbacks)
                    schedule.tick(now + 30, session={'open': True}, **callbacks)
                send.assert_called_once()
                schedule.tick(7200.25, session={'open': True}, **callbacks)
                self.assertEqual([self.identity(asset, hour) for hour in (3600, 7200)],
                                 [call.kwargs['report_id'] for call in send.call_args_list])

    def test_closed_hours_and_recovery_are_skipped_including_after_restart(self):
        for asset in self.assets:
            for restart in (False, True):
                with self.subTest(asset=asset, restart=restart):
                    schedule = AssetSchedule(asset, None, 100)
                    send = Mock(return_value='sent')
                    callbacks = self.callbacks(asset, send)
                    schedule.tick(200, session={'open': False}, **callbacks)
                    if restart:
                        schedule = AssetSchedule(asset, schedule.state, 9500)
                    schedule.tick(9500, session={'open': True}, **callbacks)
                    send.assert_not_called()
                    schedule.tick(10800.25, session={'open': True}, **callbacks)
                    send.assert_called_once_with(require_usable_signal_data=True,
                        report_id=self.identity(asset, 10800), check_commands=False)

    def test_recovery_at_an_exact_hour_can_send_that_hour(self):
        for asset in self.assets:
            with self.subTest(asset=asset):
                schedule = AssetSchedule(asset, None, 100)
                send = Mock(return_value='sent')
                callbacks = self.callbacks(asset, send)
                schedule.tick(200, session={'open': False}, **callbacks)
                schedule.tick(3600, session={'open': True}, **callbacks)
                send.assert_called_once()
                self.assertEqual(self.identity(asset, 3600), send.call_args.kwargs['report_id'])

    def test_restart_drops_old_preparation_retries_and_missed_hours(self):
        for asset in self.assets:
            for retry in (None, 115):
                with self.subTest(asset=asset, retry=retry):
                    saved = dict(next_push=3600, retry_at=retry, retry_attempt=1,
                                 report_id=self.identity(asset, 0), market_open=True)
                    original = copy.deepcopy(saved)
                    schedule = AssetSchedule(asset, saved, 9500)
                    send = Mock(return_value='sent')
                    callbacks = self.callbacks(asset, send)
                    schedule.tick(9500, session={'open': True}, **callbacks)
                    send.assert_not_called()
                    self.assertIsNone(schedule.state['retry_at'])
                    self.assertEqual(10800, schedule.state['next_push'])
                    self.assertEqual(original, saved)
                    self.assertEqual(schedule.state, callbacks['save'].call_args.args[0])
                    schedule.tick(10800.25, session={'open': True}, **callbacks)
                    self.assertEqual(self.identity(asset, 10800), send.call_args.kwargs['report_id'])

    def test_legacy_resume_state_is_retired_without_sending(self):
        for asset in ('QQQ', 'VOO'):
            with self.subTest(asset=asset):
                saved = dict(next_push=300, retry_at=315, retry_attempt=1,
                             report_id=self.identity(asset, 0) + ':resume:300', market_open=True)
                schedule = AssetSchedule(asset, saved, 320)
                send = Mock(return_value='sent')
                callbacks = self.callbacks(asset, send)
                schedule.tick(320, session={'open': True}, **callbacks)
                send.assert_not_called()
                self.assertIsNone(schedule.state['report_id'])
                self.assertIsNone(schedule.state['retry_at'])
                self.assertEqual(3600, schedule.state['next_push'])
                schedule.tick(3600.25, session={'open': True}, **callbacks)
                self.assertEqual(self.identity(asset, 3600), send.call_args.kwargs['report_id'])

    def test_same_hour_retry_survives_restart_and_remains_bounded(self):
        for asset in self.assets:
            with self.subTest(asset=asset):
                schedule = AssetSchedule(asset, None, 100, retries=1)
                send = Mock(side_effect=['retryable', 'retryable', 'sent'])
                callbacks = self.callbacks(asset, send)
                schedule.tick(3600.25, session={'open': True}, **callbacks)
                schedule = AssetSchedule(asset, schedule.state, 3605, retries=1)
                schedule.tick(3605, session={'open': True}, **callbacks)
                self.assertEqual(1, send.call_count)
                schedule.tick(3615.25, session={'open': True}, **callbacks)
                schedule.tick(3650, session={'open': True}, **callbacks)
                self.assertEqual([self.identity(asset, 3600)] * 3,
                                 [call.kwargs['report_id'] for call in send.call_args_list])
                self.assertEqual([True, True, False],
                                 [call.kwargs['require_usable_signal_data'] for call in send.call_args_list])
                self.assertIsNone(schedule.state['retry_at'])

    def test_sent_and_unknown_reports_are_not_retried_after_restart(self):
        for asset in self.assets:
            for result in ('sent', 'unknown'):
                with self.subTest(asset=asset, result=result):
                    schedule = AssetSchedule(asset, None, 100)
                    send = Mock(return_value=result)
                    callbacks = self.callbacks(asset, send)
                    schedule.tick(3600.25, session={'open': True}, **callbacks)
                    schedule = AssetSchedule(asset, schedule.state, 4000)
                    schedule.tick(4000, session={'open': True}, **callbacks)
                    send.assert_called_once()


if __name__ == '__main__':
    unittest.main()
