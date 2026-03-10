"""
scrapers/news.py

Global news scraper for missing children.

Features:
  - 120+ RSS/news feeds across 60+ countries in 20+ languages
  - Resolution detection: marks DB records as resolved when found/rescued news appears
  - Cross-references article names against the local database
  - Deduplicates articles by URL
"""

import re
import time
from datetime import datetime

import feedparser

from database.models import NewsArticle, MissingPerson
from scrapers.base import BaseScraper
from utils.helpers import clean_text, extract_names_from_text

# ---------------------------------------------------------------------------
# Resolution keywords — if ANY appear in title/summary, child was likely found
# ---------------------------------------------------------------------------

RESOLUTION_KEYWORDS = [
    # English
    "found safe", "found alive", "has been found", "was found", "been located",
    "safely recovered", "has been recovered", "been recovered", "returned home",
    "reunited with", "has been reunited", "located safe", "amber alert cancelled",
    "amber alert canceled", "amber alert resolved", "child found", "teen found",
    "juvenile found", "safely returned", "no longer missing", "case closed",
    "child recovered", "children recovered", "rescued", "safe recovery",
    "found unharmed", "found unhurt", "recovered safely", "missing child found",
    "missing teen found", "missing girl found", "missing boy found",
    # Spanish
    "fue encontrado", "fue encontrada", "encontrado con vida", "encontrada con vida",
    "ha sido localizado", "ha sido localizada", "niño encontrado", "niña encontrada",
    "alerta amber cancelada", "fue rescatado", "fue rescatada", "aparecio con vida",
    "apareció con vida", "fue hallado", "fue hallada", "menor fue encontrado",
    # Portuguese
    "foi encontrado", "foi encontrada", "foi localizado", "foi localizada",
    "crianca encontrada", "menor encontrado", "resgatado", "resgatada",
    "criança foi encontrada", "menor foi localizado", "foi achado", "foi achada",
    # French
    "a ete retrouve", "a ete retrouvee", "enfant retrouve", "enfant retrouvee",
    "retrouve sain", "retrouvee saine", "alerte enlevement annulee",
    "a été retrouvé", "a été retrouvée", "enfant retrouvé", "retrouvé sain et sauf",
    # German
    "wurde gefunden", "ist gefunden", "kind gefunden", "vermisstes kind gefunden",
    "wohlbehalten aufgefunden", "wurde gerettet", "wieder gefunden",
    "ist wieder aufgetaucht", "lebend gefunden",
    # Italian
    "e stato trovato", "e stata trovata", "bambino trovato", "ritrovato sano",
    "è stato trovato", "è stata trovata", "ritrovata sana e salva",
    "minore ritrovato", "bimbo ritrovato",
    # Dutch
    "is gevonden", "kind gevonden", "vermist kind gevonden", "veilig gevonden",
    "is teruggevonden", "werd teruggevonden", "veilig terug",
    # Turkish
    "bulundu", "kurtarildi", "kayip cocuk bulundu", "sağ bulundu",
    "kayıp çocuk bulundu", "güvende bulundu",
    # Polish
    "odnaleziono", "dziecko odnalezione", "bezpiecznie odnalezione",
    "zaginiete dziecko znalezione", "odnaleziono zaginione",
    # Russian
    "najden", "najdena", "rebenok najden", "найден", "найдена",
    "ребёнок найден", "найден живым", "нашли живой",
    # Greek
    "vrethike", "entopiistike", "βρέθηκε", "εντοπίστηκε", "βρέθηκε ζωντανό",
    # Arabic
    "tm alethwr", "othir ala", "تم العثور", "عثر على",
    "وجد الطفل", "تم إنقاذ",
    # Japanese
    "hakken", "hogo", "発見", "保護", "無事発見",
    # Korean
    "balgyon", "gujo", "발견", "구조", "무사발견",
    # Swahili
    "amepatikana", "ameokoka", "mtoto amepatikana",
    # Hindi/Urdu
    "mil gaya", "mil gayi", "bachcha mila", "surakshit mila",
    # Tagalog
    "nahanap", "natagpuan", "ligtas na natagpuan",
    # Indonesian/Malay
    "ditemukan", "berhasil ditemukan", "anak ditemukan selamat",
    # Thai
    "พบแล้ว", "พบตัวแล้ว", "พบเด็กแล้ว",
    # Vietnamese
    "da tim thay", "tim thay roi", "duoc tim thay",
    # Amharic/Ethiopian
    "tegegnual", "tewetual",
]

MISSING_KEYWORDS = [
    # English
    "missing", "abducted", "amber alert", "last seen", "endangered",
    "runaway", "kidnapped", "abduction", "disappear", "disappeared",
    "missing child", "missing teen", "missing girl", "missing boy",
    "missing juvenile", "child missing", "teen missing",
    # Spanish
    "desaparecido", "desaparecida", "menor desaparecido", "nino desaparecido",
    "niño desaparecido", "niña desaparecida", "alerta amber", "secuestrado",
    "secuestrada", "menor extraviado", "niño extraviado",
    # Portuguese
    "desaparecida", "desaparecido", "crianca desaparecida", "criança desaparecida",
    "menor desaparecido", "criança sumiu",
    # French
    "disparu", "disparue", "enfant disparu", "alerte enlevement",
    "enfant disparu", "alerte enlèvement", "mineur disparu",
    # German
    "vermisst", "entfuhrt", "kindesentfuhrung", "vermisstes kind",
    "entführt", "Kindesentführung", "Kind vermisst",
    # Italian
    "scomparso", "scomparsa", "bambino scomparso", "sequestrato",
    "minore scomparso", "bimbo scomparso",
    # Dutch
    "vermist", "ontvoerd", "vermist kind", "kind vermist",
    # Turkish
    "kayip", "kayip cocuk", "kacirildi", "kayıp", "kayıp çocuk", "kaçırıldı",
    # Polish
    "zaginięcie", "zaginięte dziecko", "uprowadzenie", "dziecko zaginęło",
    # Russian
    "propal", "propala", "пропал", "пропала", "пропал ребёнок",
    "пропавший ребенок", "похищен", "похищена",
    # Greek
    "exafanisi", "apagogi", "εξαφάνιση", "απαγωγή", "εξαφανίστηκε παιδί",
    # Arabic
    "mfqwd", "tfl mfqwd", "طفل مفقود", "مفقود", "اختطاف طفل",
    # Japanese
    "yukuefumei", "yukai", "行方不明", "誘拐", "子供行方不明",
    # Korean
    "siljeong", "napchi", "실종", "납치", "아동 실종",
    # Swahili
    "amepotea", "mtoto aliyepotea", "kutekwa",
    # Hindi
    "lapata", "bachcha lapata", "apaharan",
    # Tagalog
    "nawawala", "nawawalang bata", "kidnap",
    # Indonesian/Malay
    "hilang", "anak hilang", "diculik", "kanak-kanak hilang",
    # Thai
    "หาย", "เด็กหาย", "ลักพาตัว",
    # Vietnamese
    "mat tich", "tre em mat tich", "bắt cóc",
    # Amharic
    "teHede", "lijoch teHed",
    # Hausa (Nigeria)
    "ya bace", "yaron ya bace",
    # Yoruba (Nigeria)
    "sonu", "omo sonu",
    # Zulu/Xhosa (South Africa)
    "ulahlekile", "umntwana ulahlekile",
]

# ---------------------------------------------------------------------------
# Feed list: (label, url)
# 120+ feeds across 60+ countries
# ---------------------------------------------------------------------------

FEEDS = [

    # ========================================================
    # NORTH AMERICA
    # ========================================================

    # --- USA ---
    ("USA: Amber Alert",
     "https://news.google.com/rss/search?q=%22amber+alert%22&hl=en-US&gl=US&ceid=US:en"),
    ("USA: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+%22last+seen%22&hl=en-US&gl=US&ceid=US:en"),
    ("USA: NCMEC news",
     "https://news.google.com/rss/search?q=NCMEC+missing&hl=en-US&gl=US&ceid=US:en"),
    ("USA: Child abduction",
     "https://news.google.com/rss/search?q=%22child+abduction%22&hl=en-US&gl=US&ceid=US:en"),
    ("USA: Child found safe",
     "https://news.google.com/rss/search?q=%22child+found+safe%22+OR+%22amber+alert+canceled%22&hl=en-US&gl=US&ceid=US:en"),
    ("USA: Missing teen",
     "https://news.google.com/rss/search?q=%22missing+teen%22+police&hl=en-US&gl=US&ceid=US:en"),
    ("USA: Missing girl",
     "https://news.google.com/rss/search?q=%22missing+girl%22+police+%22last+seen%22&hl=en-US&gl=US&ceid=US:en"),
    ("USA: Child trafficking",
     "https://news.google.com/rss/search?q=%22child+trafficking%22+arrest&hl=en-US&gl=US&ceid=US:en"),
    ("USA: NCMEC RSS",
     "https://www.missingkids.org/missingkids/servlet/XmlServlet?act=rss&missType=child&LanguageCountry=en_US"),

    # --- Canada ---
    ("Canada: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+RCMP&hl=en-CA&gl=CA&ceid=CA:en"),
    ("Canada: Amber alert",
     "https://news.google.com/rss/search?q=%22amber+alert%22+canada&hl=en-CA&gl=CA&ceid=CA:en"),
    ("Canada: Child found",
     "https://news.google.com/rss/search?q=%22missing+child%22+found+canada&hl=en-CA&gl=CA&ceid=CA:en"),

    # --- Mexico ---
    ("Mexico: Alerta amber",
     "https://news.google.com/rss/search?q=%22alerta+amber%22&hl=es-419&gl=MX&ceid=MX:es-419"),
    ("Mexico: Nino desaparecido",
     "https://news.google.com/rss/search?q=ni%C3%B1o+desaparecido&hl=es-419&gl=MX&ceid=MX:es-419"),
    ("Mexico: Menor encontrado",
     "https://news.google.com/rss/search?q=menor+encontrado+sano&hl=es-419&gl=MX&ceid=MX:es-419"),
    ("Mexico: Trata infantil",
     "https://news.google.com/rss/search?q=trata+menores+mexico&hl=es-419&gl=MX&ceid=MX:es-419"),

    # ========================================================
    # CENTRAL AMERICA & CARIBBEAN
    # ========================================================

    ("Guatemala: Nino desaparecido",
     "https://news.google.com/rss/search?q=ni%C3%B1o+desaparecido+guatemala&hl=es-419&gl=GT&ceid=GT:es-419"),
    ("Guatemala: Alerta alba-kenneth",
     "https://news.google.com/rss/search?q=alerta+alba+kenneth+guatemala&hl=es-419&gl=GT&ceid=GT:es-419"),
    ("Honduras: Menor desaparecido",
     "https://news.google.com/rss/search?q=menor+desaparecido+honduras&hl=es-419&gl=HN&ceid=HN:es-419"),
    ("El Salvador: Nino desaparecido",
     "https://news.google.com/rss/search?q=ni%C3%B1o+desaparecido+el+salvador&hl=es-419&gl=SV&ceid=SV:es-419"),
    ("Costa Rica: Menor desaparecido",
     "https://news.google.com/rss/search?q=menor+desaparecido+costa+rica&hl=es-419&gl=CR&ceid=CR:es-419"),
    ("Panama: Nino desaparecido",
     "https://news.google.com/rss/search?q=ni%C3%B1o+desaparecido+panama&hl=es-419&gl=PA&ceid=PA:es-419"),
    ("Nicaragua: Menor desaparecido",
     "https://news.google.com/rss/search?q=menor+desaparecido+nicaragua&hl=es-419&gl=NI&ceid=NI:es-419"),
    ("Jamaica: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+jamaica&hl=en&gl=JM&ceid=JM:en"),
    ("Jamaica: Child found",
     "https://news.google.com/rss/search?q=missing+child+found+jamaica&hl=en&gl=JM&ceid=JM:en"),
    ("Haiti: Enfant disparu",
     "https://news.google.com/rss/search?q=enfant+disparu+haiti&hl=fr&gl=HT&ceid=HT:fr"),
    ("Dominican Republic: Menor desaparecido",
     "https://news.google.com/rss/search?q=menor+desaparecido+republica+dominicana&hl=es-419&gl=DO&ceid=DO:es-419"),
    ("Trinidad: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+trinidad&hl=en&gl=TT&ceid=TT:en"),

    # ========================================================
    # SOUTH AMERICA
    # ========================================================

    ("Colombia: Menor desaparecido",
     "https://news.google.com/rss/search?q=menor+desaparecido&hl=es-419&gl=CO&ceid=CO:es-419"),
    ("Colombia: Nino encontrado",
     "https://news.google.com/rss/search?q=menor+desaparecido+encontrado+colombia&hl=es-419&gl=CO&ceid=CO:es-419"),
    ("Venezuela: Nino desaparecido",
     "https://news.google.com/rss/search?q=ni%C3%B1o+desaparecido+venezuela&hl=es-419&gl=VE&ceid=VE:es-419"),
    ("Ecuador: Menor desaparecido",
     "https://news.google.com/rss/search?q=menor+desaparecido+ecuador&hl=es-419&gl=EC&ceid=EC:es-419"),
    ("Ecuador: Alerta temprana",
     "https://news.google.com/rss/search?q=alerta+temprana+menor+ecuador&hl=es-419&gl=EC&ceid=EC:es-419"),
    ("Peru: Menor desaparecido",
     "https://news.google.com/rss/search?q=menor+desaparecido+peru&hl=es-419&gl=PE&ceid=PE:es-419"),
    ("Bolivia: Nino desaparecido",
     "https://news.google.com/rss/search?q=ni%C3%B1o+desaparecido+bolivia&hl=es-419&gl=BO&ceid=BO:es-419"),
    ("Argentina: Nino desaparecido",
     "https://news.google.com/rss/search?q=ni%C3%B1o+desaparecido&hl=es-419&gl=AR&ceid=AR:es-419"),
    ("Argentina: Alerta busqueda",
     "https://news.google.com/rss/search?q=alerta+busqueda+menor+argentina&hl=es-419&gl=AR&ceid=AR:es-419"),
    ("Chile: Menor desaparecido",
     "https://news.google.com/rss/search?q=menor+desaparecido+chile&hl=es-419&gl=CL&ceid=CL:es-419"),
    ("Uruguay: Menor desaparecido",
     "https://news.google.com/rss/search?q=menor+desaparecido+uruguay&hl=es-419&gl=UY&ceid=UY:es-419"),
    ("Paraguay: Nino desaparecido",
     "https://news.google.com/rss/search?q=ni%C3%B1o+desaparecido+paraguay&hl=es-419&gl=PY&ceid=PY:es-419"),
    ("Brazil: Crianca desaparecida",
     "https://news.google.com/rss/search?q=crian%C3%A7a+desaparecida&hl=pt-BR&gl=BR&ceid=BR:pt-419"),
    ("Brazil: Menor encontrado",
     "https://news.google.com/rss/search?q=menor+desaparecido+encontrado&hl=pt-BR&gl=BR&ceid=BR:pt-419"),
    ("Brazil: Crianca sequestrada",
     "https://news.google.com/rss/search?q=crian%C3%A7a+sequestrada+brasil&hl=pt-BR&gl=BR&ceid=BR:pt-419"),

    # ========================================================
    # WESTERN EUROPE
    # ========================================================

    # --- UK & Ireland ---
    ("UK: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+police&hl=en-GB&gl=GB&ceid=GB:en"),
    ("UK: Child found safe",
     "https://news.google.com/rss/search?q=%22missing+child%22+%22found+safe%22&hl=en-GB&gl=GB&ceid=GB:en"),
    ("UK: Child abduction",
     "https://news.google.com/rss/search?q=%22child+abduction%22&hl=en-GB&gl=GB&ceid=GB:en"),
    ("UK: Child trafficking arrest",
     "https://news.google.com/rss/search?q=%22child+trafficking%22+arrested&hl=en-GB&gl=GB&ceid=GB:en"),
    ("Ireland: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+garda&hl=en-IE&gl=IE&ceid=IE:en"),

    # --- France & Belgium ---
    ("France: Enfant disparu",
     "https://news.google.com/rss/search?q=enfant+disparu&hl=fr&gl=FR&ceid=FR:fr"),
    ("France: Enfant retrouve",
     "https://news.google.com/rss/search?q=enfant+retrouv%C3%A9&hl=fr&gl=FR&ceid=FR:fr"),
    ("France: Alerte enlevement",
     "https://news.google.com/rss/search?q=alerte+enl%C3%A8vement&hl=fr&gl=FR&ceid=FR:fr"),
    ("Belgium: Enfant disparu",
     "https://news.google.com/rss/search?q=enfant+disparu&hl=fr&gl=BE&ceid=BE:fr"),
    ("Belgium: Kind vermist",
     "https://news.google.com/rss/search?q=kind+vermist+belgie&hl=nl&gl=BE&ceid=BE:nl"),

    # --- Germany & Austria & Switzerland ---
    ("Germany: Vermisstes Kind",
     "https://news.google.com/rss/search?q=vermisstes+Kind&hl=de&gl=DE&ceid=DE:de"),
    ("Germany: Kind gefunden",
     "https://news.google.com/rss/search?q=vermisstes+Kind+gefunden&hl=de&gl=DE&ceid=DE:de"),
    ("Germany: Kindesentfuhrung",
     "https://news.google.com/rss/search?q=Kindesentf%C3%BChrung&hl=de&gl=DE&ceid=DE:de"),
    ("Austria: Kind vermisst",
     "https://news.google.com/rss/search?q=Kind+vermisst+%C3%96sterreich&hl=de&gl=AT&ceid=AT:de"),
    ("Switzerland: Kind vermisst",
     "https://news.google.com/rss/search?q=Kind+vermisst+Schweiz&hl=de&gl=CH&ceid=CH:de"),

    # --- Spain & Portugal ---
    ("Spain: Menor desaparecido",
     "https://news.google.com/rss/search?q=menor+desaparecido&hl=es&gl=ES&ceid=ES:es"),
    ("Spain: Nino encontrado",
     "https://news.google.com/rss/search?q=menor+desaparecido+encontrado+espa%C3%B1a&hl=es&gl=ES&ceid=ES:es"),
    ("Portugal: Crianca desaparecida",
     "https://news.google.com/rss/search?q=crian%C3%A7a+desaparecida+portugal&hl=pt-PT&gl=PT&ceid=PT:pt-150"),
    ("Portugal: Menor desaparecido",
     "https://news.google.com/rss/search?q=menor+desaparecido+portugal&hl=pt-PT&gl=PT&ceid=PT:pt-150"),

    # --- Italy ---
    ("Italy: Bambino scomparso",
     "https://news.google.com/rss/search?q=bambino+scomparso&hl=it&gl=IT&ceid=IT:it"),
    ("Italy: Bambino ritrovato",
     "https://news.google.com/rss/search?q=bambino+scomparso+ritrovato&hl=it&gl=IT&ceid=IT:it"),
    ("Italy: Minore scomparso",
     "https://news.google.com/rss/search?q=minore+scomparso+italia&hl=it&gl=IT&ceid=IT:it"),

    # --- Netherlands ---
    ("Netherlands: Vermist kind",
     "https://news.google.com/rss/search?q=vermist+kind&hl=nl&gl=NL&ceid=NL:nl"),
    ("Netherlands: Kind teruggevonden",
     "https://news.google.com/rss/search?q=vermist+kind+teruggevonden&hl=nl&gl=NL&ceid=NL:nl"),

    # --- Scandinavia ---
    ("Sweden: Forsvunnet barn",
     "https://news.google.com/rss/search?q=f%C3%B6rsvunnet+barn&hl=sv&gl=SE&ceid=SE:sv"),
    ("Norway: Savnet barn",
     "https://news.google.com/rss/search?q=savnet+barn+politi&hl=no&gl=NO&ceid=NO:no"),
    ("Denmark: Forsvundet barn",
     "https://news.google.com/rss/search?q=forsvundet+barn&hl=da&gl=DK&ceid=DK:da"),
    ("Finland: Kadonnut lapsi",
     "https://news.google.com/rss/search?q=kadonnut+lapsi&hl=fi&gl=FI&ceid=FI:fi"),

    # ========================================================
    # EASTERN EUROPE
    # ========================================================

    ("Poland: Zaginięcie dziecka",
     "https://news.google.com/rss/search?q=zaginięcie+dziecka&hl=pl&gl=PL&ceid=PL:pl"),
    ("Poland: Dziecko odnalezione",
     "https://news.google.com/rss/search?q=zaginięte+dziecko+odnalezione&hl=pl&gl=PL&ceid=PL:pl"),
    ("Romania: Copil disparut",
     "https://news.google.com/rss/search?q=copil+disp%C4%83rut+romania&hl=ro&gl=RO&ceid=RO:ro"),
    ("Romania: Copil gasit",
     "https://news.google.com/rss/search?q=copil+disp%C4%83rut+g%C4%83sit&hl=ro&gl=RO&ceid=RO:ro"),
    ("Ukraine: Dytyna znykla",
     "https://news.google.com/rss/search?q=%D0%B4%D0%B8%D1%82%D0%B8%D0%BD%D0%B0+%D0%B7%D0%BD%D0%B8%D0%BA%D0%BB%D0%B0&hl=uk&gl=UA&ceid=UA:uk"),
    ("Bulgaria: Dete izchezna",
     "https://news.google.com/rss/search?q=%D0%B4%D0%B5%D1%82%D0%B5+%D0%B8%D0%B7%D1%87%D0%B5%D0%B7%D0%BD%D0%B0+%D0%91%D1%8A%D0%BB%D0%B3%D0%B0%D1%80%D0%B8%D1%8F&hl=bg&gl=BG&ceid=BG:bg"),
    ("Czech Republic: Pohresovane dite",
     "https://news.google.com/rss/search?q=pohřešované+dítě&hl=cs&gl=CZ&ceid=CZ:cs"),
    ("Hungary: Eltunt gyerek",
     "https://news.google.com/rss/search?q=elt%C5%B1nt+gyerek&hl=hu&gl=HU&ceid=HU:hu"),
    ("Serbia: Nestalo dete",
     "https://news.google.com/rss/search?q=nestalo+dete+srbija&hl=sr&gl=RS&ceid=RS:sr"),
    ("Croatia: Nestalo dijete",
     "https://news.google.com/rss/search?q=nestalo+dijete+hrvatska&hl=hr&gl=HR&ceid=HR:hr"),
    ("Russia: Propal rebyonok",
     "https://news.google.com/rss/search?q=%D0%BF%D1%80%D0%BE%D0%BF%D0%B0%D0%BB+%D1%80%D0%B5%D0%B1%D1%91%D0%BD%D0%BE%D0%BA&hl=ru&gl=RU&ceid=RU:ru"),
    ("Russia: Rebyonok najden",
     "https://news.google.com/rss/search?q=%D1%80%D0%B5%D0%B1%D1%91%D0%BD%D0%BE%D0%BA+%D0%BD%D0%B0%D0%B9%D0%B4%D0%B5%D0%BD&hl=ru&gl=RU&ceid=RU:ru"),
    ("Moldova: Copil disparut",
     "https://news.google.com/rss/search?q=copil+disp%C4%83rut+moldova&hl=ro&gl=MD&ceid=MD:ro"),
    ("Belarus: Rebyonok propal",
     "https://news.google.com/rss/search?q=%D1%80%D0%B5%D0%B1%D1%91%D0%BD%D0%BE%D0%BA+%D0%BF%D1%80%D0%BE%D0%BF%D0%B0%D0%BB+%D0%B1%D0%B5%D0%BB%D0%B0%D1%80%D1%83%D1%81%D1%8C&hl=ru&gl=BY&ceid=BY:ru"),
    ("Albania: Femije zhdukur",
     "https://news.google.com/rss/search?q=f%C3%ABmij%C3%AB+zhdukur+shqiperi&hl=sq&gl=AL&ceid=AL:sq"),

    # ========================================================
    # MIDDLE EAST & NORTH AFRICA
    # ========================================================

    ("Turkey: Kayip cocuk",
     "https://news.google.com/rss/search?q=kay%C4%B1p+%C3%A7ocuk&hl=tr&gl=TR&ceid=TR:tr"),
    ("Turkey: Cocuk bulundu",
     "https://news.google.com/rss/search?q=kay%C4%B1p+%C3%A7ocuk+bulundu&hl=tr&gl=TR&ceid=TR:tr"),
    ("Saudi Arabia: Tifl mafqoud",
     "https://news.google.com/rss/search?q=%D8%B7%D9%81%D9%84+%D9%85%D9%81%D9%82%D9%88%D8%AF&hl=ar&gl=SA&ceid=SA:ar"),
    ("Egypt: Tifl mafqoud",
     "https://news.google.com/rss/search?q=%D8%B7%D9%81%D9%84+%D9%85%D9%81%D9%82%D9%88%D8%AF+%D9%85%D8%B5%D8%B1&hl=ar&gl=EG&ceid=EG:ar"),
    ("Egypt: Tifl wujid",
     "https://news.google.com/rss/search?q=%D8%B7%D9%81%D9%84+%D9%85%D9%81%D9%82%D9%88%D8%AF+%D8%B9%D8%AB%D8%B1+%D9%85%D8%B5%D8%B1&hl=ar&gl=EG&ceid=EG:ar"),
    ("Morocco: Tifl mafqoud",
     "https://news.google.com/rss/search?q=%D8%B7%D9%81%D9%84+%D9%85%D9%81%D9%82%D9%88%D8%AF+%D8%A7%D9%84%D9%85%D8%BA%D8%B1%D8%A8&hl=ar&gl=MA&ceid=MA:ar"),
    ("Algeria: Tifl mafqoud",
     "https://news.google.com/rss/search?q=%D8%B7%D9%81%D9%84+%D9%85%D9%81%D9%82%D9%88%D8%AF+%D8%A7%D9%84%D8%AC%D8%B2%D8%A7%D8%A6%D8%B1&hl=ar&gl=DZ&ceid=DZ:ar"),
    ("Jordan: Tifl mafqoud",
     "https://news.google.com/rss/search?q=%D8%B7%D9%81%D9%84+%D9%85%D9%81%D9%82%D9%88%D8%AF+%D8%A7%D9%84%D8%A3%D8%B1%D8%AF%D9%86&hl=ar&gl=JO&ceid=JO:ar"),
    ("Iraq: Tifl mafqoud",
     "https://news.google.com/rss/search?q=%D8%B7%D9%81%D9%84+%D9%85%D9%81%D9%82%D9%88%D8%AF+%D8%A7%D9%84%D8%B9%D8%B1%D8%A7%D9%82&hl=ar&gl=IQ&ceid=IQ:ar"),
    ("Lebanon: Tifl mafqoud",
     "https://news.google.com/rss/search?q=%D8%B7%D9%81%D9%84+%D9%85%D9%81%D9%82%D9%88%D8%AF+%D9%84%D8%A8%D9%86%D8%A7%D9%86&hl=ar&gl=LB&ceid=LB:ar"),
    ("UAE: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+UAE&hl=en&gl=AE&ceid=AE:en"),
    ("Pakistan: Bachcha lapata",
     "https://news.google.com/rss/search?q=%D8%A8%DA%86%DB%81+%D9%84%D8%A7%D9%BE%D8%AA%D8%A7+%D9%BE%D8%A7%DA%A9%D8%B3%D8%AA%D8%A7%D9%86&hl=ur&gl=PK&ceid=PK:ur"),
    ("Pakistan: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+pakistan&hl=en&gl=PK&ceid=PK:en"),
    ("Iran: Kudak gomshode",
     "https://news.google.com/rss/search?q=%DA%A9%D9%88%D8%AF%DA%A9+%DA%AF%D9%85%D8%B4%D8%AF%D9%87+%D8%A7%DB%8C%D8%B1%D8%A7%D9%86&hl=fa&gl=IR&ceid=IR:fa"),
    ("Israel: Yeled ne'elam",
     "https://news.google.com/rss/search?q=%D7%99%D7%9C%D7%93+%D7%A0%D7%A2%D7%9C%D7%9D&hl=iw&gl=IL&ceid=IL:iw"),

    # ========================================================
    # SUB-SAHARAN AFRICA
    # ========================================================

    ("Nigeria: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+nigeria&hl=en-NG&gl=NG&ceid=NG:en"),
    ("Nigeria: Child abduction",
     "https://news.google.com/rss/search?q=%22child+abduction%22+nigeria&hl=en-NG&gl=NG&ceid=NG:en"),
    ("Nigeria: Child trafficking",
     "https://news.google.com/rss/search?q=%22child+trafficking%22+nigeria&hl=en-NG&gl=NG&ceid=NG:en"),
    ("Nigeria: Child found",
     "https://news.google.com/rss/search?q=missing+child+found+nigeria&hl=en-NG&gl=NG&ceid=NG:en"),
    ("Kenya: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+kenya&hl=en-KE&gl=KE&ceid=KE:en"),
    ("Kenya: Mtoto aliyepotea",
     "https://news.google.com/rss/search?q=mtoto+aliyepotea+kenya&hl=sw&gl=KE&ceid=KE:sw"),
    ("South Africa: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+%22south+africa%22&hl=en-ZA&gl=ZA&ceid=ZA:en"),
    ("South Africa: Child found",
     "https://news.google.com/rss/search?q=missing+child+found+%22south+africa%22&hl=en-ZA&gl=ZA&ceid=ZA:en"),
    ("South Africa: Child trafficking",
     "https://news.google.com/rss/search?q=%22child+trafficking%22+%22south+africa%22&hl=en-ZA&gl=ZA&ceid=ZA:en"),
    ("Ghana: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+ghana&hl=en&gl=GH&ceid=GH:en"),
    ("Ghana: Child trafficking",
     "https://news.google.com/rss/search?q=%22child+trafficking%22+ghana&hl=en&gl=GH&ceid=GH:en"),
    ("Uganda: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+uganda&hl=en&gl=UG&ceid=UG:en"),
    ("Tanzania: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+tanzania&hl=en&gl=TZ&ceid=TZ:en"),
    ("Tanzania: Mtoto aliyepotea",
     "https://news.google.com/rss/search?q=mtoto+aliyepotea+tanzania&hl=sw&gl=TZ&ceid=TZ:sw"),
    ("Ethiopia: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+ethiopia&hl=en&gl=ET&ceid=ET:en"),
    ("Zimbabwe: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+zimbabwe&hl=en&gl=ZW&ceid=ZW:en"),
    ("Zambia: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+zambia&hl=en&gl=ZM&ceid=ZM:en"),
    ("Malawi: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+malawi&hl=en&gl=MW&ceid=MW:en"),
    ("Cameroon: Enfant disparu",
     "https://news.google.com/rss/search?q=enfant+disparu+cameroun&hl=fr&gl=CM&ceid=CM:fr"),
    ("Democratic Republic of Congo: Enfant disparu",
     "https://news.google.com/rss/search?q=enfant+disparu+congo&hl=fr&gl=CD&ceid=CD:fr"),
    ("Senegal: Enfant disparu",
     "https://news.google.com/rss/search?q=enfant+disparu+s%C3%A9n%C3%A9gal&hl=fr&gl=SN&ceid=SN:fr"),
    ("Ivory Coast: Enfant disparu",
     "https://news.google.com/rss/search?q=enfant+disparu+c%C3%B4te+d%27ivoire&hl=fr&gl=CI&ceid=CI:fr"),
    ("Mozambique: Crianca desaparecida",
     "https://news.google.com/rss/search?q=crian%C3%A7a+desaparecida+mo%C3%A7ambique&hl=pt-PT&gl=MZ&ceid=MZ:pt-150"),
    ("Angola: Crianca desaparecida",
     "https://news.google.com/rss/search?q=crian%C3%A7a+desaparecida+angola&hl=pt-PT&gl=AO&ceid=AO:pt-150"),

    # ========================================================
    # SOUTH ASIA
    # ========================================================

    ("India: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+police+india&hl=en-IN&gl=IN&ceid=IN:en"),
    ("India: Child found",
     "https://news.google.com/rss/search?q=%22missing+child%22+found+india&hl=en-IN&gl=IN&ceid=IN:en"),
    ("India: Child trafficking",
     "https://news.google.com/rss/search?q=%22child+trafficking%22+india+arrested&hl=en-IN&gl=IN&ceid=IN:en"),
    ("India: Bachcha lapata",
     "https://news.google.com/rss/search?q=%E0%A4%AC%E0%A4%9A%E0%A5%8D%E0%A4%9A%E0%A4%BE+%E0%A4%B2%E0%A4%BE%E0%A4%AA%E0%A4%A4%E0%A4%BE&hl=hi&gl=IN&ceid=IN:hi"),
    ("Bangladesh: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+bangladesh&hl=en&gl=BD&ceid=BD:en"),
    ("Nepal: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+nepal&hl=en&gl=NP&ceid=NP:en"),
    ("Sri Lanka: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+%22sri+lanka%22&hl=en&gl=LK&ceid=LK:en"),
    ("Afghanistan: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+afghanistan&hl=en&gl=AF&ceid=AF:en"),

    # ========================================================
    # SOUTHEAST ASIA
    # ========================================================

    ("Philippines: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+philippines&hl=en-PH&gl=PH&ceid=PH:en"),
    ("Philippines: Nawawalang bata",
     "https://news.google.com/rss/search?q=nawawalang+bata+pilipinas&hl=fil&gl=PH&ceid=PH:fil"),
    ("Philippines: Child trafficking",
     "https://news.google.com/rss/search?q=%22child+trafficking%22+philippines&hl=en-PH&gl=PH&ceid=PH:en"),
    ("Indonesia: Anak hilang",
     "https://news.google.com/rss/search?q=anak+hilang+indonesia&hl=id&gl=ID&ceid=ID:id"),
    ("Indonesia: Anak ditemukan",
     "https://news.google.com/rss/search?q=anak+hilang+ditemukan+indonesia&hl=id&gl=ID&ceid=ID:id"),
    ("Malaysia: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+malaysia&hl=en-MY&gl=MY&ceid=MY:en"),
    ("Malaysia: Kanak-kanak hilang",
     "https://news.google.com/rss/search?q=kanak-kanak+hilang+malaysia&hl=ms&gl=MY&ceid=MY:ms"),
    ("Thailand: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+thailand&hl=en&gl=TH&ceid=TH:en"),
    ("Thailand: Dek haai",
     "https://news.google.com/rss/search?q=%E0%B9%80%E0%B8%94%E0%B9%87%E0%B8%81%E0%B8%AB%E0%B8%B2%E0%B8%A2&hl=th&gl=TH&ceid=TH:th"),
    ("Vietnam: Tre em mat tich",
     "https://news.google.com/rss/search?q=tr%E1%BA%BB+em+m%E1%BA%A5t+t%C3%ADch&hl=vi&gl=VN&ceid=VN:vi"),
    ("Cambodia: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+cambodia&hl=en&gl=KH&ceid=KH:en"),
    ("Myanmar: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+myanmar&hl=en&gl=MM&ceid=MM:en"),

    # ========================================================
    # EAST ASIA & PACIFIC
    # ========================================================

    ("Japan: Yukue fumei kodomo",
     "https://news.google.com/rss/search?q=%E8%A1%8C%E6%96%B9%E4%B8%8D%E6%98%8E+%E5%AD%90%E4%BE%9B&hl=ja&gl=JP&ceid=JP:ja"),
    ("Japan: Kodomo hogo",
     "https://news.google.com/rss/search?q=%E5%AD%90%E4%BE%9B+%E7%99%BA%E8%A6%8B+%E4%BF%9D%E8%AD%B7&hl=ja&gl=JP&ceid=JP:ja"),
    ("South Korea: Siljeong adong",
     "https://news.google.com/rss/search?q=%EC%8B%A4%EC%A2%85+%EC%95%84%EB%8F%99&hl=ko&gl=KR&ceid=KR:ko"),
    ("South Korea: Adong balgyon",
     "https://news.google.com/rss/search?q=%EC%95%84%EB%8F%99+%EC%8B%A4%EC%A2%85+%EB%B0%9C%EA%B2%AC&hl=ko&gl=KR&ceid=KR:ko"),
    ("China: Shizong ertong",
     "https://news.google.com/rss/search?q=%E5%A4%B1%E8%B8%AA%E5%84%BF%E7%AB%A5&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"),
    ("Taiwan: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+taiwan&hl=zh-TW&gl=TW&ceid=TW:zh-Hant"),
    ("Australia: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+australia&hl=en-AU&gl=AU&ceid=AU:en"),
    ("Australia: Child found",
     "https://news.google.com/rss/search?q=%22missing+child%22+%22found+safe%22+australia&hl=en-AU&gl=AU&ceid=AU:en"),
    ("New Zealand: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+%22new+zealand%22&hl=en-NZ&gl=NZ&ceid=NZ:en"),
    ("Papua New Guinea: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+%22papua+new+guinea%22&hl=en&gl=PG&ceid=PG:en"),

    # ========================================================
    # GREECE (SPECIAL — high GMCN case volume)
    # ========================================================

    ("Greece: Exafanisi paidiou",
     "https://news.google.com/rss/search?q=%CE%B5%CE%BE%CE%B1%CF%86%CE%AC%CE%BD%CE%B9%CF%83%CE%B7+%CF%80%CE%B1%CE%B9%CE%B4%CE%B9%CE%BF%CF%8D&hl=el&gl=GR&ceid=GR:el"),
    ("Greece: Pedi vrethike",
     "https://news.google.com/rss/search?q=%CF%80%CE%B1%CE%B9%CE%B4%CE%AF+%CE%B2%CF%81%CE%AD%CE%B8%CE%B7%CE%BA%CE%B5&hl=el&gl=GR&ceid=GR:el"),
    ("Greece: Apagogi paidiou",
     "https://news.google.com/rss/search?q=%CE%B1%CF%80%CE%B1%CE%B3%CF%89%CE%B3%CE%AE+%CF%80%CE%B1%CE%B9%CE%B4%CE%B9%CE%BF%CF%8D&hl=el&gl=GR&ceid=GR:el"),

]


class NewsScraper(BaseScraper):
    name = "news"

    def run(self) -> dict:
        found = new = updated = errors = 0
        resolved_count = 0

        self.logger.info("Starting Global News scrape (%d feeds, 60+ countries)...", len(FEEDS))

        for feed_name, feed_url in FEEDS:
            try:
                articles = self._fetch_feed(feed_name, feed_url)
                if articles:
                    self.logger.info("%s: %d articles", feed_name, len(articles))
                found += len(articles)

                for article in articles:
                    try:
                        _, created = self._upsert_article(article)
                        if created:
                            new += 1
                        else:
                            updated += 1
                        resolved = self._cross_reference(article)
                        resolved_count += resolved
                    except Exception as exc:
                        self.logger.error("Article save failed: %s", exc)
                        errors += 1

            except Exception as exc:
                self.logger.warning("Feed '%s' failed: %s", feed_name, exc)
                errors += 1

        self.logger.info(
            "News done. found=%d new=%d updated=%d resolved=%d errors=%d",
            found, new, updated, resolved_count, errors,
        )
        return {"found": found, "new": new, "updated": updated,
                "resolved": resolved_count, "errors": errors}

    def _fetch_feed(self, feed_name: str, url: str) -> list[dict]:
        time.sleep(0.8)
        feed     = feedparser.parse(url)
        articles = []

        for entry in feed.entries:
            title    = clean_text(entry.get("title", ""))
            summary  = clean_text(entry.get("summary", "") or entry.get("description", ""))
            combined = f"{title} {summary}".lower()

            is_missing    = any(kw.lower() in combined for kw in MISSING_KEYWORDS)
            is_resolution = any(kw.lower() in combined for kw in RESOLUTION_KEYWORDS)

            if not (is_missing or is_resolution):
                continue

            published = None
            if entry.get("published_parsed"):
                try:
                    published = datetime(*entry.published_parsed[:6])
                except Exception:
                    pass

            articles.append({
                "url":           entry.get("link", ""),
                "title":         title,
                "summary":       summary,
                "source_name":   feed_name,
                "published_at":  published,
                "is_resolution": is_resolution,
            })

        return articles

    def _upsert_article(self, article: dict) -> tuple:
        url = article.get("url", "")
        if not url:
            raise ValueError("Article missing URL")

        title   = article.get("title", "")
        summary = article.get("summary", "")
        names   = extract_names_from_text(f"{title} {summary}")

        update_data = {
            "title":           title,
            "summary":         summary,
            "source_name":     article["source_name"],
            "published_at":    article["published_at"],
            "names_mentioned": ", ".join(names) if names else None,
        }

        instance = self.db.query(NewsArticle).filter_by(url=url).first()
        created  = False
        if instance is None:
            instance = NewsArticle(url=url, **update_data)
            self.db.add(instance)
            created = True
        else:
            for k, v in update_data.items():
                setattr(instance, k, v)
        self.db.commit()
        return instance, created

    def _cross_reference(self, article: dict) -> int:
        """
        Match names in article against DB records.
        If the article is a resolution (found/rescued), mark matching
        DB records as is_resolved=True and store resolution notes.
        Returns count of newly resolved records.
        """
        title          = article.get("title", "")
        summary        = article.get("summary", "")
        is_resolution  = article.get("is_resolution", False)
        names          = extract_names_from_text(f"{title} {summary}")
        resolved_count = 0

        for full_name in names:
            parts = full_name.split()
            if len(parts) < 2:
                continue
            first, last = parts[0], parts[-1]

            matches = self.db.query(MissingPerson).filter(
                MissingPerson.first_name.ilike(first),
                MissingPerson.last_name.ilike(last),
                (MissingPerson.age_at_disappearance < 18) |
                (MissingPerson.age_at_disappearance == None),
            ).all()

            for match in matches:
                if is_resolution and not match.is_resolved:
                    match.is_resolved      = True
                    match.resolution_notes = (
                        f"Found via news: {title[:200]} | "
                        f"Source: {article['source_name']} | "
                        f"URL: {article['url']}"
                    )
                    self.db.commit()
                    resolved_count += 1
                    self.logger.warning(
                        "RESOLVED: '%s' marked found — '%s' [%s/%s]",
                        full_name, title[:80], match.source, match.source_id,
                    )
                elif not is_resolution and not match.is_resolved:
                    self.logger.warning(
                        "MATCH: '%s' in '%s' -> DB record %s/%s",
                        full_name, title[:70], match.source, match.source_id,
                    )

        return resolved_count
