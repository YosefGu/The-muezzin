import speech_recognition as sr
from pydub import AudioSegment

class SpeechRecognition():
    _r = sr.Recognizer()

    # @classmethod
    def __convert_to_wav(path):
        if not path.lower().endswith(".wav"):
            audio = AudioSegment.from_file(path)
            file_name = path.split('.')[0]
            audio.export(f"{file_name}.wav", format="wav")
            return audio
        return path
    
    def __extrack_text_from_audio(cls, path):
        with sr.AudioFile(path) as source:
            audio_data = cls._r.record(source)

            try:
            # Recognize speech using Google Web Speech API (default)
                text = cls._r.recognize_google(audio_data)
                print("Transcribed Text:", text)
            except sr.UnknownValueError:
                print("Speech Recognition could not understand audio")
            except sr.RequestError as e:
                print(f"Could not request results from Google Speech Recognition service; {e}")

            


    


        