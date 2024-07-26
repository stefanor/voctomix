from gi.repository import Gst, GObject

from vocto.audio_codecs import construct_audio_decoder_pipeline
from vocto.video_codecs import construct_video_decoder_pipeline

from lib.config import Config
from lib.sources.avsource import AVSource

ALL_AUDIO_CAPS = Gst.Caps.from_string('audio/x-raw')
ALL_VIDEO_CAPS = Gst.Caps.from_string('video/x-raw')


class SRTAVSource(AVSource):

    timer_resolution = 0.5

    def __init__(self, name, has_audio=True, has_video=True):
        super().__init__('SRTAVSource', name, has_audio, has_video, show_no_signal=True)

        self.location = Config.getLocation(name)
        self.name = name
        self.connected = True

        self.build_pipeline()

    def port(self):
        return f"SRT: {self.name}"

    def num_connections(self):
        return int(self.connected)

    def __str__(self):
        return f'SRTAVSource[{self.name}] {self.location}'

    def build_source(self):
        videodecoder = construct_video_decoder_pipeline(self.section())
        audiodecoder = construct_audio_decoder_pipeline(self.section())
        pipe = f"""
            srtsrc
                name=srtsrc-{self.name}
                uri={self.location}
                wait-for-connection=false
            ! tsdemux
                name=demux-{self.name}
            ! queue
                name=queue-srtsrc-video-{self.name}
            ! {videodecoder}
            """

        # maybe add deinterlacer
        if self.build_deinterlacer():
            pipe += f"! {self.build_deinterlacer()}\n"

        pipe += f"""\
            ! videoconvert
            ! videoscale
            ! videorate
                name=vout-{self.name}
            demux-{self.name}.
            ! queue
                name=queue-srtsrc-audio-{self.name}
            ! {audiodecoder}
            ! audioconvert
            ! audioresample
                name=aout-{self.name}
            videotestsrc name=vfake-{self.name}
            audiotestsrc name=afake-{self.name}
            """

        return pipe

    def attach(self, pipeline):
        super().attach(pipeline)
        self.log.debug("connecting to pads")

        self.srtsrc = pipeline.get_by_name(f'srtsrc-{self.name}')
        self.srtsrc.get_static_pad("src").add_probe(
            Gst.PadProbeType.EVENT_DOWNSTREAM | Gst.PadProbeType.BLOCK, self.on_pad_event)

        # subscribe to creation of dynamic pads in tsdemux
        self.demux = pipeline.get_by_name(f'demux-{self.name}')
        #self.demux.set_state(Gst.State.READY)
        self.demux.connect('pad-added', self.on_pad_added)

        # remember queues the demux is connected to to reconnect them when necessary
        self.queue_audio = pipeline.get_by_name(f'queue-srtsrc-audio-{self.name}')
        self.queue_video = pipeline.get_by_name(f'queue-srtsrc-video-{self.name}')

        self.src = pipeline.get_by_name(f'src-{self.name}')

        self.vout =  pipeline.get_by_name(f'vout-{self.name}')
        self.aout =  pipeline.get_by_name(f'aout-{self.name}')
        self.vfake = pipeline.get_by_name(f'vfake-{self.name}')
        self.afake = pipeline.get_by_name(f'afake-{self.name}')
        self.queue_source_audio = pipeline.get_by_name(f'queue-source-audio-{self.name}')
        self.vcapsfilter = pipeline.get_by_name(f'vcapsfilter-{self.name}')

    def on_pad_event(self, pad, info):
        if info.get_event().type == Gst.EventType.EOS:
            self.log.warning('scheduling source restart')
            self.connected = False
            GObject.idle_add(self.restart)

        return Gst.PadProbeReturn.PASS

    def on_pad_added(self, demux, pad):
        caps = pad.query_caps(None)
        self.log.debug('demuxer added pad w/ caps: %s', caps.to_string())

        if self.has_audio and caps.can_intersect(ALL_AUDIO_CAPS):
            self.log.debug('new demuxer-pad is an audio-pad, '
                           'testing against configured audio-caps')
            if not caps.can_intersect(self.audio_caps):
                self.log.warning('the incoming connection presented '
                                 'an audio-stream that is not compatible '
                                 'to the configured caps')
                self.log.warning('   incoming caps:   %s', caps.to_string())
                self.log.warning('   configured caps: %s',
                                 self.audio_caps.to_string())

        elif self.has_video and caps.can_intersect(ALL_VIDEO_CAPS):
            self.log.debug('new demuxer-pad is a video-pad, '
                           'testing against configured video-caps')
            if not caps.can_intersect(self.video_caps):
                self.log.warning('the incoming connection presented '
                                 'a video-stream that is not compatible '
                                 'to the configured caps')
                self.log.warning('   incoming caps:   %s', caps.to_string())
                self.log.warning('   configured caps: %s',
                                 self.video_caps.to_string())

            self.test_and_warn_interlace_mode(caps)

        # relink demux with following audio and video queues
        if not pad.is_linked():
            print("HELLO")
            self.demux.link(self.queue_audio)
            self.demux.link(self.queue_video)

            self.vcapsfilter.get_static_pad('sink').set_blocked(True)
            self.vfake.get_static_pad('src').unlink(self.vcapsfilter)
            self.vout.get_static_pad('src').link(self.vcapsfilter)

            self.queue_source_audio.get_static_pad('sink').set_blocked(True)
            self.afake.get_static_pad('src').unlink(self.queue_source_audio)
            self.aout.get_static_pad('src').link(self.queue_source_audio)

            self.demux.set_state(Gst.State.PLAYING)
            self.queue_source_audio.get_static_pad('sink').set_blocked(False)
            self.vcapsfilter.get_static_pad('sink').set_blocked(False)

        self.connected = True

    def restart(self):
        self.log.debug('restarting source \'%s\'', self.name)
        self.srtsrc.set_state(Gst.State.READY)
        self.demux.set_state(Gst.State.READY)
        self.demux.set_state(Gst.State.PLAYING)
        self.srtsrc.set_state(Gst.State.PLAYING)

    def build_audioport(self):
        return f'afake-{self.name}.'

    def build_videoport(self):
        return f'vfake-{self.name}.'
