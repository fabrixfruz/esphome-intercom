"""Constants for VoIP Stack integration."""

import json
from pathlib import Path

DOMAIN = "voip_stack"
SIP_CALL_ENDED_EVENT = "voip_stack.call_ended"
CONF_ASSIST_INTENTS = "assist_intents"
CONF_ASSIST_ENDPOINT_ENABLED = "assist_endpoint_enabled"
CONF_ASSIST_EXTENSION = "assist_extension"
CONF_ASSIST_PIPELINE = "assist_pipeline"
CONF_ASSIST_ADVANCED_CALL_CONTEXT = "assist_advanced_call_context"
# Multilingua per-turno: il motore TTS scelto qui viene chiamato DIRETTAMENTE
# (bypassando pipeline.tts_language, fisso per pipeline in HA core) con la
# lingua rilevata di volta in volta sul testo generato dall'agente
# conversazionale. Se vuoto, il comportamento resta quello originale
# (lingua fissa della pipeline).
CONF_ASSIST_DYNAMIC_TTS_ENGINE = "assist_dynamic_tts_engine"
# Elenco whitelist di codici lingua attesi (es. "it,en,de,fr"), separati da
# virgola. Restringere il rilevamento a questo insieme riduce i falsi
# positivi tipici del language-detection su frasi brevi.
CONF_ASSIST_DYNAMIC_TTS_LANGUAGES = "assist_dynamic_tts_languages"
# Entita' da osservare per sapere se una chiamata reale originata verso un
# residente (dopo la risoluzione di un interno via Assist) e' stata
# risposta o e' scaduta - servono per annunciare l'esito all'ospite
# invece di lasciarlo in attesa senza risposta.
CONF_ASSIST_VOIP_STATE_ENTITY = "assist_voip_state_entity"
CONF_ASSIST_VOIP_REASON_ENTITY = "assist_voip_reason_entity"
# Voce esplicita per il motore TTS dinamico (es. "kore" per Google AI TTS/
# Gemini) - vuoto lascia il motore usare la propria voce di default.
# Solo alcuni motori (es. tts.google_ai_tts) supportano questa opzione;
# viene applicata solo al bypass dinamico, mai al TTS di apertura o di
# fallback della pipeline, che potrebbero usare un motore diverso.
CONF_ASSIST_DYNAMIC_TTS_VOICE = "assist_dynamic_tts_voice"
# Saluto di apertura fisso (es. "Reception Residencial Test") - se
# compilato, sostituisce la battuta generata da Gemini per il primo
# turno: nessuna chiamata all'LLM, nessuna variabilita' di lingua/
# formulazione nel momento piu' delicato (primo contatto con l'ospite).
# Se vuoto, il comportamento resta quello di sempre (saluto generato).
CONF_ASSIST_FIXED_GREETING = "assist_fixed_greeting"
# Mappa lingua->voce per motori dove la lingua e' incorporata nel nome
# della voce stessa (es. Deepgram Aura: "aura-2-livia-it") invece di un
# parametro separato come Gemini. Formato: "it:voce_it,en:voce_en,...".
# Ha priorita' su CONF_ASSIST_DYNAMIC_TTS_VOICE (voce singola) quando
# presente per la lingua rilevata; altrimenti si ricade su quest'ultima.
CONF_ASSIST_DYNAMIC_TTS_VOICES = "assist_dynamic_tts_voices"
CONF_DEBUG_MODE = "debug_mode"
# Keep the persisted key stable for configured entries created before the SIP
# video profile graduated from preview status.
CONF_SIP_VIDEO = "experimental_sip_video"
CONF_VIDEO_TRANSCODING = "video_transcoding_enabled"
CONF_VIDEO_CAMERA_SEND = "video_camera_send_enabled"
CONF_HA_SOFTPHONE_DND = "ha_softphone_dnd"
CONF_HA_SOFTPHONE_EXTENSION = "ha_softphone_extension"
CONF_HA_SOFTPHONE_RING_GROUP = "ha_softphone_ring_group"
CONF_HA_SOFTPHONE_CONFERENCE_GROUP = "ha_softphone_conference_group"
CONF_HA_SOFTPHONE_CONFERENCE_RING = "ha_softphone_conference_ring"
CONF_REGISTRAR_ENABLED = "sip_registrar_enabled"
CONF_SIP_ACCOUNTS = "sip_accounts"
CONF_PHONEBOOK_CONTACTS = "phonebook_contacts"
CONF_TRUNK_ENABLED = "trunk_enabled"
CONF_TRUNK_TRANSPORT = "trunk_transport"
CONF_TRUNK_SERVER = "trunk_server"
CONF_TRUNK_PORT = "trunk_port"
CONF_TRUNK_DOMAIN = "trunk_domain"
CONF_TRUNK_USERNAME = "trunk_username"
CONF_TRUNK_AUTH_USERNAME = "trunk_auth_username"
CONF_TRUNK_PASSWORD = "trunk_password"
CONF_TRUNK_EXPIRES = "trunk_register_expires"
CONF_TRUNK_OUTBOUND_PROXY = "trunk_outbound_proxy"
CONF_TRUNK_INBOUND_DEFAULT_TARGET = "trunk_inbound_default_target"
CONF_TRUNK_INBOUND_MODE = "trunk_inbound_mode"
CONF_AUTOMATION_ROUTING_ENABLED = "automation_routing_enabled"
CONF_TRUNK_DTMF_ENABLED = "trunk_dtmf_enabled"
CONF_TRUNK_DTMF_TIMEOUT_MS = "trunk_dtmf_timeout_ms"
CONF_TRUNK_DTMF_TERMINATOR = "trunk_dtmf_terminator"
TRUNK_INBOUND_MODE_DIRECT = "direct"
TRUNK_INBOUND_MODE_DTMF = "dtmf"

# Version from manifest.json
_MANIFEST = Path(__file__).parent / "manifest.json"
with open(_MANIFEST, encoding="utf-8") as _f:
    INTEGRATION_VERSION = json.load(_f).get("version", "0.0.0")

# Frontend URL base for serving the Lovelace card
URL_BASE = "/voip-stack"
HA_PEER_FALLBACK_NAME = "voip-stack"
HA_SOFTPHONE_DEVICE_ID = "__voip_stack_ha_softphone__"
HA_SOFTPHONE_ENDPOINT_ENTITY_ID = "sensor.voip_stack_ha_softphone_voip_endpoint"
HA_SOFTPHONE_CALL_STATE_ENTITY_ID = "sensor.voip_stack_call_state"

VOIP_STACK_SIP_PORT = 5060
VOIP_STACK_RTP_PORT = 40000
