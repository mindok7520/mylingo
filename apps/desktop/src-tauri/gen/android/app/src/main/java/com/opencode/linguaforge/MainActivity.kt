package com.opencode.linguaforge

import android.os.Bundle
import android.speech.tts.TextToSpeech
import android.webkit.JavascriptInterface
import android.webkit.WebView
import androidx.activity.enableEdgeToEdge
import java.util.Locale

class MainActivity : TauriActivity() {
  private var textToSpeech: TextToSpeech? = null
  private var ttsReady = false
  private var pendingText: Pair<String, String>? = null

  override fun onCreate(savedInstanceState: Bundle?) {
    enableEdgeToEdge()
    super.onCreate(savedInstanceState)

    textToSpeech = TextToSpeech(this) { status ->
      ttsReady = status == TextToSpeech.SUCCESS
      if (ttsReady) {
        pendingText?.let { (text, languageTag) ->
          speakNow(text, languageTag)
          pendingText = null
        }
      }
    }
  }

  override fun onWebViewCreate(webView: WebView) {
    super.onWebViewCreate(webView)
    webView.addJavascriptInterface(AndroidTtsBridge(), "LinguaForgeAndroidTts")
  }

  override fun onDestroy() {
    textToSpeech?.stop()
    textToSpeech?.shutdown()
    textToSpeech = null
    super.onDestroy()
  }

  private fun speakNow(text: String, languageTag: String): Boolean {
    val engine = textToSpeech ?: return false
    val locale = Locale.forLanguageTag(languageTag)
    engine.language = locale
    val result = engine.speak(text, TextToSpeech.QUEUE_FLUSH, null, "linguaforge-tts")
    return result == TextToSpeech.SUCCESS
  }

  inner class AndroidTtsBridge {
    @JavascriptInterface
    fun isAvailable(): Boolean {
      return ttsReady && textToSpeech != null
    }

    @JavascriptInterface
    fun speak(text: String, languageTag: String): Boolean {
      if (text.isBlank()) {
        return false
      }

      if (!ttsReady) {
        pendingText = text to languageTag
        return true
      }

      return speakNow(text, languageTag)
    }
  }
}
