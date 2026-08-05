#include <QDir>
#include <QHash>
#include <QFile>
#include <QProcess>
#include <QTimeZone>
#include <QJsonObject>
#include <QJsonDocument>
#include <QNetworkReply>
#include <QCoreApplication>
#include <QNetworkAccessManager>

qint64 dateStringToTimestamp(const QString &dateStr)
{   // yyyy-MM-dd -> Unix时间戳
    if (dateStr.isEmpty()) return 0;
    const QDateTime dt(QDate::fromString(dateStr, "yyyy-MM-dd").startOfDay(QTimeZone::utc()));
    return dt.toSecsSinceEpoch();
}

bool generateEpisodeSQL(const QString &inputFile, QTextStream &out)
{
    const qint64 now = QDateTime::currentSecsSinceEpoch();
    QHash<int, bool> subjectHasFuture;
    {
        QFile file(inputFile);
        if (!file.open(QIODevice::ReadOnly | QIODevice::Text)) return false;
        QTextStream stream(&file);
        while (!stream.atEnd()) {
            QJsonObject obj = QJsonDocument::fromJson(stream.readLine().toUtf8()).object();
            int subject_id = obj["subject_id"].toInt();
            if (subject_id == 184017) continue;
            QString airdateStr = obj["airdate"].toString();
            if (airdateStr.isEmpty()) continue;
            if (dateStringToTimestamp(airdateStr) > now) subjectHasFuture[subject_id] = true;
        }
    }
    QFile file(inputFile);
    if (!file.open(QIODevice::ReadOnly | QIODevice::Text)) return false;
    QTextStream stream(&file);
    int count = 0;
    out << "BEGIN TRANSACTION;";
    while (!stream.atEnd()) {
        QJsonObject obj = QJsonDocument::fromJson(stream.readLine().toUtf8()).object();
        int subject_id = obj["subject_id"].toInt();
        if (subject_id == 184017) continue;
        if (!subjectHasFuture.value(subject_id, false)) continue;
        if (count % 1000 == 0) {
            if (count > 0) {
                out << ";";
                out << "COMMIT;";
                out << "BEGIN TRANSACTION;";
            }
            out << "INSERT OR REPLACE INTO episode_public_date (subject_id, episode_id, airdate, sort, type) VALUES";
        } else out << ",";
        out << "(" << subject_id << "," << obj["id"].toInt() << "," << dateStringToTimestamp(obj["airdate"].toString()) << "," << static_cast<int>(obj["sort"].toDouble() * 10.0) << "," << obj["type"].toInt() << ")";
        ++count;
    }
    if (count > 0) {
        out << ";";
        out << "COMMIT;";
    } else {
        out << "-- No episode data inserted.";
        out << "COMMIT;";
    }
    return true;
}

bool generateRelationsSQL(const QString &inputFile, QTextStream &out)
{
    QFile file(inputFile);
    if (!file.open(QIODevice::ReadOnly | QIODevice::Text)) return false;
    QHash<int, int> countMap;
    QTextStream stream(&file);
    while (!stream.atEnd()) {
        QJsonObject obj = QJsonDocument::fromJson(stream.readLine().toUtf8()).object();
        if (obj["relation_type"].toInt() != 1003) continue;
        int subject_id = obj["subject_id"].toInt();
        countMap[subject_id] = countMap.value(subject_id, 0) + 1;
    }
    out << "BEGIN TRANSACTION;";
    int count = 0;
    for (auto it = countMap.begin(); it != countMap.end(); ++it) {
        if (count % 1000 == 0) {
            if (count > 0) {
                out << ";";
                out << "COMMIT;";
                out << "BEGIN TRANSACTION;";
            }
            out << "INSERT OR REPLACE INTO subject_relations (subject_id, count) VALUES";

        } else out << ",";
        out << "(" << it.key() << "," << it.value() << ")";
        ++count;
    }
    if (count > 0) {
        out << ";";
        out << "COMMIT;";
    } else {
        out << "-- No relations data inserted.";
        out << "COMMIT;";
    }
    return true;
}

QString fetchBrowserDownloadUrl()
{
    QNetworkAccessManager manager;
    const QNetworkRequest request(QUrl("https://raw.githubusercontent.com/bangumi/Archive/refs/heads/master/aux/latest.json"));
    QNetworkReply *reply = manager.get(request);
    QEventLoop loop;
    QObject::connect(reply, &QNetworkReply::finished, &loop, &QEventLoop::quit);
    loop.exec();
    if (reply->error() != QNetworkReply::NoError) {
        reply->deleteLater();
        return {};
    }
    const QByteArray data = reply->readAll();
    reply->deleteLater();
    return QJsonDocument::fromJson(data).object()["browser_download_url"].toString();
}

bool downloadFile(const QString &url, const QString &localPath)
{   // 下载文件
    QNetworkAccessManager manager;
    const QNetworkRequest request(url);
    QNetworkReply *reply = manager.get(request);
    QEventLoop loop;
    QFile file(localPath);
    if (!file.open(QIODevice::WriteOnly)) {
        reply->deleteLater();
        return false;
    }
    QObject::connect(reply, &QNetworkReply::readyRead, [&file, reply] {file.write(reply->readAll());});
    QObject::connect(reply, &QNetworkReply::finished, &loop, &QEventLoop::quit);
    loop.exec();
    file.close();
    const bool success = reply->error() == QNetworkReply::NoError;
    reply->deleteLater();
    return success;
}

bool extractZip(const QString &zipPath, const QString &destDir, QString &episodePath, QString &subjectRelationsPath)
{
    const QDir dir;
    if (!dir.mkpath(destDir)) return false;
    QProcess unzip;
    QStringList args;
    args << "-o" << zipPath << "-d" << destDir;
    unzip.start("unzip", args);
    if (!unzip.waitForFinished()) return false;
    episodePath = destDir + "/episode.jsonlines";
    subjectRelationsPath = destDir + "/subject-relations.jsonlines";
    return true;
}

int main(int argc, char *argv[])
{
    QCoreApplication app(argc, argv);
    const QString zipPath = QCoreApplication::applicationDirPath() + "/data.zip";
    if (!downloadFile(fetchBrowserDownloadUrl(), zipPath)) return 1;
    QString episodeFile, subjectRelationsFile;
    if (!extractZip(zipPath, "extracted", episodeFile, subjectRelationsFile)) return 1;
    QFile sqlFile(QCoreApplication::applicationDirPath() + "/bangumi_data.sql");
    if (!sqlFile.open(QIODevice::WriteOnly | QIODevice::Text)) return 1;
    QTextStream out(&sqlFile);
    out << "CREATE TABLE IF NOT EXISTS episode_public_date (subject_id INTEGER, episode_id INTEGER, airdate INTEGER, sort INTEGER, type INTEGER, PRIMARY KEY (subject_id, episode_id));";
    out << "CREATE TABLE IF NOT EXISTS subject_relations (subject_id INTEGER PRIMARY KEY, count INTEGER);";
    if (!generateEpisodeSQL(episodeFile, out)) return 1;
    if (!generateRelationsSQL(subjectRelationsFile, out)) return 1;
    sqlFile.close();
    return 0;
}