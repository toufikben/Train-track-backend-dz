# WinRah — مذكرة تدقيق ونشر PR14

**التاريخ:** 24 أغسطس 2026. **النطاق:** Backend PR14، Render، وفحوصات القراءة فقط. لا تشمل هذه المذكرة فتح الكتابة أو seed أو تغيير Supabase.

## النتيجة التنفيذية

تم دمج PR14 في Backend `main` ثم نشره يدويًا على Render. deployment هو `dep-da6bnh2fngtc73937fr0`، وأصبح `live` على SHA الكامل `3aea44da515508ea81d2e25da36b5f8d7df0f553`. سجل Render يثبت نجاح build وhealth check، و`/version` يعيد `git_sha=3aea44da5155`، وهو أول 12 خانة من SHA المنشور.

| الفحص | النتيجة | الدليل المقروء |
|---|---:|---|
| Render deployment | ✅ | `dep-da6bnh2fngtc73937fr0` = `live` |
| Commit | ✅ | `3aea44da515508ea81d2e25da36b5f8d7df0f553` |
| Build وstartup | ✅ | Build successful، `psycopg=3.3.4`، Postgres active |
| `/version` | ✅ | HTTP 200، `storage=postgres-postgis`، `git_sha=3aea44da5155` |
| `/health` | ✅ | HTTP 200، `status=ok`، 28 محطة و105 رحلات |
| `/stations` و`/trips` | ✅ | HTTP 200؛ 28 و105 صفًا |
| GeoJSON | ✅ | HTTP 200؛ FeatureCollection فيها 3 features مرجعية/مشتقة |
| `/admin/health` بلا مفتاح | ✅ | HTTP 401 |
| عمليات كتابة | ⏸️ | لم تُرسل POST/PUT/DELETE، ولم يحدث seed أو حذف |

## إغلاق عناصر مراجعة PR14

| العنصر | الحالة | تفسير مضبوط بالنطاق |
|---|---:|---|
| R-03 | ✅ مغلق | version provenance مثبت بـ`git_sha` المطابق للـdeployment live. |
| R-12 | ✅ مغلق كإصلاح كود منشور | حدود telemetry ورفض NaN/Inf وfuture epoch-ms clamp مثبتة بالاختبارات وبالنشر. |
| R-13 | ✅ مغلق كإصلاح ونشر | health صادق، وPostgres active، وcounts محمّلة من التخزين. |
| R-15 | ⚠️ مغلق شرطيًا | مسار deferred route references محمي وموسوم، لكن الإغلاق التشغيلي النهائي مشروط بنتيجة R-21. |
| R-16 | ✅ مغلق كإصلاح ونشر | batching في adapters وrehydration اتجاهي انعكسا في الكود، والسجل حمّل 1476 trip-stop لـ105 رحلات. |
| R-18 | ✅ مغلق من ناحية الحماية | `RETURN` fallback إلى `INBOUND`، مع بقاء أي خط مؤجل خارج الخدمة المنشورة. |
| C5 | ✅ مغلق كإصلاح PR14 فقط | الحالة مثبتة على SHA المنشور؛ لا تعني وجود live telemetry ولا إغلاق مانع `C-05` العام الخاص بالبيانات الحية. |

## ما بقي مفتوحًا

| العنصر | الحالة | السبب والتصحيح |
|---|---:|---|
| R-21 | ⏸️ P1 | لم يُثبت runtime CTE عبر SQL proxy بمفتاح حقيقي؛ السبب هو منع كشف الأسرار والكتابة السحابية. التصحيح: اختبار SELECT محدود داخل مسار إداري آمن بعد تدوير الأسرار وخطة رجوع. |
| R-20 | ⏸️ P2 | batching لا يساوي idempotency عند retry أو partial failure. التصحيح: transaction/idempotency key وتصميم نتائج retry. |
| R-22 | ⚠️ P3 | limiter داخل العملية يحتاج bounded cache/TTL عند ارتفاع تنوع IPs. |
| R-23 | ⚠️ P3 | timestamps القديمة جدًا لا تُرفض صراحة. التصحيح: نافذة freshness موثقة قبل live telemetry. |
| R-15 | ⚠️ مشروط | ينتظر R-21؛ لا يُعلن مغلقًا تشغيليًا. |
| C-05 العام | ⏸️ P0 | لا توجد observations حية، والكتابة محجوبة. التصحيح يتطلب Auth وowner-RLS وoutbox/idempotency ومصدر GPS حقيقي. |

## الخطوط المؤجلة

| المرجع | الحالة | الحد المسموح حاليًا |
|---|---:|---|
| D-01 — ثنية–تيزي وزو | ⚠️ تقدّم مرجعي فقط | manifest/catalog محلي provisional؛ لا seed ولا خدمة تشغيلية. |
| D-02 — وادي عيسي | ⚠️ تقدّم مرجعي فقط | لا توجد geometry/محطة حالية موثقة كفاية للنشر. |
| D-06 — الإحداثيات | ⚠️ تقدّم مرجعي فقط | بعض anchors تقريبية وموسومة provisional؛ يلزم GIS/OSM مراجَع أو مصدر رسمي. |

تبقى معلومات Thenia–Tizi–Oued Aïssi والمطار خارج Supabase وخارج الإعلان التشغيلي إلى أن تتوفر من SNTF قائمة حالية للمحطات بالترتيب، وUUIDs canonical، والهندسة، والجداول وأرقام القطارات. لا يجوز تحويل بيانات المحاكاة المحلية إلى train IDs حية.

## حدود السلامة

ظل `WINRAH_PUBLIC_WRITES_ENABLED=false`. لم تُجرَ أي كتابة أو حذف أو migration أو seed أو تنظيف سحابي، ولم تُطبع أي قيمة سرية. Supabase Free لا يوفر scheduled backups أو PITR، وlogical snapshot الحالي جزئي وليس restore-grade؛ لذلك لا يزال الحذف وفتح الكتابة محجوبين.

## المراجع

[1]: https://train-api-uep7.onrender.com/version "WinRah Render — version and git SHA"
[2]: https://train-api-uep7.onrender.com/health "WinRah Render — health and storage counts"
[3]: https://www.sntf.dz/index.php/communication/item/49-mise-en-service-du-troncon-tizi-ouzou-oued-aissi "SNTF — Mise en service du tronçon Tizi Ouzou–Oued Aïssi"
