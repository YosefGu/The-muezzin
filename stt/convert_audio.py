import speech_recognition as sr
from pydub import AudioSegment
from logger import Logger

my_logger = Logger.get_logger()


class ConvertAudio():
    _r = sr.Recognizer()


    @classmethod
    def speech_to_text(self, path):
        updated_path = self.__convert_to_wav(self, path)
        text = self.__extrack_text_from_audio(self, updated_path)
        return text


    def __convert_to_wav(self, path):
        if not path.lower().endswith(".wav"):
            audio = AudioSegment.from_file(path)
            file_name = path.split('.')[0]
            audio.export(f"{file_name}.wav", format="wav")
            return audio
        return path
    
    def __extrack_text_from_audio(self, path):
        with sr.AudioFile(path) as source:
            audio_data = self._r.record(source)
            try:
                text = self._r.recognize_google(audio_data)
                my_logger.info("The audio file was successfully transcribed.")
                return text
            except sr.UnknownValueError:
                my_logger.error("Speech Recognition could not understand audio")
            except sr.RequestError as e:
                my_logger.error(f"Could not request results from Google Speech Recognition service; {e}")

            


    


        