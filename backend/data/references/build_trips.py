#!/usr/bin/env python3
"""Build trip_stops JSON from SNTF official timetable images (2026-07-01).

Only stations registered in public.stations are included (no fabricated stops).
"""
import json

# Direction lists: station names as they appear in the SNTF image, in route order.
ZERALDA_ALGER = [
    "زرالدة", "سيدي عبد الله -ج", "سيدي عبد الله", "تسالة المرجة", "بنر توتة",
    "بابا علي", "عين النعجة", "جسر قسنطينة", "الحراش", "خروبة",
    "حسين داي", "الورشات", "آغا",
]  # "الجزائر" (central station) not in DB → skipped
ALGER_ZERALDA = [
    "آغا", "الورشات", "حسين داي", "خروبة", "الحراش", "جسر قسنطينة",
    "عين النعجة", "بابا علي", "بنر توتة", "تسالة المرجة",
    "سيدي عبد الله", "سيدي عبد الله -ج", "زرالدة",
]
ALGER_THENIA = [
    "آغا", "الورشات", "حسين داي", "خروبة", "الحراش", "وادي السمار",
    "باب الزوار", "الدار البيضاء", "رويبة", "راغابة", "بودواو",
    "قورصو", "بومرداس", "الثنية",
]
THENIA_ALGER = [
    "الثنية", "بومرداس", "قورصو", "بودواو", "راغابة", "رويبة",
    "الدار البيضاء", "باب الزوار", "وادي السمار", "الحراش",
    "خروبة", "حسين داي", "الورشات", "آغا",
]
ALGER_AFFROUN = [
    "آغا", "الورشات", "حسين داي", "خروبة", "الحراش", "جسر قسنطينة",
    "عين النعجة", "بابا علي", "بنر توتة", "بوفاريك", "بني مراد",
    "البليدة", "شفة", "موزاية", "العفرون",
]
AFFROUN_ALGER = [
    "العفرون", "موزاية", "شفة", "البليدة", "بني مراد", "بوفاريك",
    "بنر توتة", "بابا علي", "عين النعجة", "جسر قسنطينة",
    "الحراش", "خروبة", "حسين داي", "الورشات", "آغا",
]

# Arabic image name → station id in public.stations
# Stations NOT registered in public.stations — MUST be skipped (no fabrication):
UNREGISTERED = set()

NAME2ID = {
    "آغا": "st-aga",
    "الورشات": "st-ateliers",
    "حسين داي": "st-hdey",
    "خروبة": "st-caroubier",
    "الحراش": "st-elhar",
    "جسر قسنطينة": "st-gue",
    "عين النعجة": "st-naadja",
    "بابا علي": "st-babaali",
    "بنر توتة": "st-birtouta",
    "تسالة المرجة": "st-tessala",
    "سيدي عبد الله -ج": "st-sidi-univ",
    "سيدي عبد الله": "st-sidi-abd",
    "زرالدة": "st-zeralda",
    "الثنية": "st-thenia",
    "رويبة": "st-rouiba",
    "الدار البيضاء": "st-darbeida",
    "باب الزوار": "st-bazzouar",
    "وادي السمار": "st-ouedsmar",
    "راغابة": "st-reghaia",
    "بودواو": "st-boudouaou",
    "قورصو": "st-corso",
    "بومرداس": "st-boumerdes",
    "بوفاريك": "st-boufarik",
    "بني مراد": "st-benimerad",
    "البليدة": "st-blida",
    "شفة": "st-cheffa",
    "موزاية": "st-mouzaia",
    "العفرون": "st-elaf",
}

# Train numbers per direction, from the six official SNTF images (01/07/2026).
# Entries like ("1051","a") mean two columns share train no. 1051 → suffix -a/-b.
TRAINS = {
    "zeralda-aga": [
        "1501", "B501", "1505", "1509", "1511", "1513", "1515", "1517",
        "1519", "1521", "1523", "1525", "1527", "1529", "B515",
    ],
    "aga-zeralda": [
        "1500", "1502", "B502", "1504", "1508", "1510", "1512", "1514",
        "1516", "1518", "1520", "1522", "1524", "1526", "1528",
    ],
    "aga-thenia": [
        "27", "105", "33", "35", "41", "47", "51", "57", "61", "67",
        "119", "15", "71", "121", ("73", "b"), ("75", "a"), ("75", "b"),
        "77", "79", "B161",
    ],
    "thenia-aga": [
        "22", "B114", "24", "28", "12", "104", "34", "40", "44",
        "46", "50", "56", "60", "62", "66", "118", "74", "76", "78",
    ],
    "aga-elaffroun": [
        "1025", "1027", "1029", "1031", "1035", "1037", "1043", "1045",
        "1049", ("1051", "a"), ("1051", "b"), "1053", "1057", ("1061", "a"),
        ("1061", "b"), "1065", ("1067", "a"), "1069",
    ],
    "elaffroun-aga": [
        "1022", "1024", ("1028", "a"), ("1028", "b"), "1032", "1034",
        "1036", "1038", "1040", "1044", "1048", "1052", "1054",
        ("1058", "a"), ("1058", "b"), "1064", "1066", "1070",
    ],
}

DIRS = {
    "zeralda-aga": ZERALDA_ALGER,
    "aga-zeralda": ALGER_ZERALDA,
    "aga-thenia": ALGER_THENIA,
    "thenia-aga": THENIA_ALGER,
    "aga-elaffroun": ALGER_AFFROUN,
    "elaffroun-aga": AFFROUN_ALGER,
}

SKIPPED_NAMES = set()
trips = {}
for droute, names in DIRS.items():
    ids = []
    for n in names:
        sid = NAME2ID.get(n)
        if n in UNREGISTERED or not sid:
            SKIPPED_NAMES.add(n)
            continue
        ids.append(sid)
    assert len(ids) >= 2, f"{droute}: too few stations"
    for tn in TRAINS[droute]:
        if isinstance(tn, tuple):
            num, suf = tn
            tid = f"{droute}-{num}-{suf}"
        else:
            tid = f"{droute}-{tn}"
        trips[tid] = ids

print(f"Total trips: {len(trips)}")
print(f"Skipped (not in DB, not fabricated): {sorted(SKIPPED_NAMES)}")

data = {
    "meta": {
        "source": "SNTF official timetable images via dz_portal",
        "source_date": "2026-07-01 (summer program)",
        "note": "Only stations registered in public.stations are included. Unregistered stations skipped.",
    },
    "trips": trips,
}
with open("/home/ubuntu/sntf_timetables/trip_stops_data.json", "w") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)
print("written")
