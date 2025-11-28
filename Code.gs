// BigQueryの設定
const PROJECT_ID = 'utoro-museum'; // 例: 'my-gcp-project-12345'
const DATASET_TABLE = 'reservations.submissions_private'; // データセット.テーブル名

/**
 * Google フォームの送信時にトリガーされる関数。
 * フォームの回答を BigQuery のテーブルに挿入します。
 *
 * @param {GoogleAppsScript.Events.FormsOnFormSubmit} e フォーム送信イベントオブジェクト
 */
function onFormSubmitToBigQuery(e) {
  Logger.log("フォーム送信イベントオブジェクト: " + JSON.stringify(e));

  // フォームの回答から必要なデータを抽出する
  // フォームの質問項目名と回答値のマッピングを作成する
  const itemResponses = e.response.getItemResponses();
  const formValues = {};
  
  for (let i = 0; i < itemResponses.length; i++) {
    const title = itemResponses[i].getItem().getTitle();
    // 回答を取得。Multiple-choice の場合、回答は文字列または文字列の配列になる。
    let response = itemResponses[i].getResponse();

    // Multichoice の「言語」は単一選択肢であると想定し、文字列として格納
    if (Array.isArray(response)) {
        response = response.join(', '); // 複数選択を許容する場合
    }
    
    // 項目名に応じて値を格納
    switch (title) {
      case '日付':
        // 回答は日付オブジェクトまたは文字列として取得される
        // BQのDATE型に合わせるため 'YYYY-MM-DD' 形式に変換
        formValues.visit_date = formatDateForBigQuery(response, "yyyy-MM-dd"); 
        break;
      case '時間':
        // 回答は時間オブジェクトまたは文字列として取得される
        // BQのTIME型に合わせるため 'HH:MM:SS' 形式に変換
        // フォームの時間項目は通常 'HH:mm:ss' 形式に近い文字列で取得されるが、
        // Dateオブジェクトから変換する際はタイムゾーンに注意。
        formValues.start_time = formatDateForBigQuery(response, "HH:mm:ss");
        Logger.log("start time input " + response);
        break;
      case '人数（一般）':
        // 数値型として格納するため、数値に変換
        formValues.num_regular = parseInt(response, 10);
        break;
      case '人数（学部生以下）':
        formValues.num_student = parseInt(response, 10);
        break;
      case '言語':
        formValues.language = response;
        break;
      default:
        // 他の項目は無視
        break;
    }
  }

  // 必須項目が全て揃っているか確認 (GASでフォーム設定に依存しない厳密なチェックは困難だが、一応ログで確認)
  Logger.log("抽出されたフォームデータ: " + JSON.stringify(formValues));
  
  // BigQueryへのINSERT文を作成
  const insertQuery = createInsertQuery(formValues);

  // BigQueryにクエリを実行
  try {
    const request = {
      query: insertQuery,
      useLegacySql: false
    };
    
    // BigQuery.Jobs.query を実行
    const queryResults = BigQuery.Jobs.query(request, PROJECT_ID);
    
    Logger.log('BigQuery INSERT 成功。ジョブID: ' + queryResults.jobReference.jobId);
    
  } catch (err) {
    Logger.log('BigQuery INSERT エラー: ' + err.message);
    // エラー発生時の処理（メール通知など）を追加しても良い
  }
}


/**
 * BigQueryへのINSERTクエリ文字列を作成します。
 *
 * @param {Object} data フォームから抽出されたデータオブジェクト
 * @returns {string} INSERTクエリ文字列
 */
function createInsertQuery(data) {
  // 文字列値をSQL安全な文字列（エスケープと引用符）に変換するヘルパー関数
  const escapeString = (str) => {
    if (str === null || str === undefined) {
        return 'NULL';
    }
    // SQLインジェクションを防ぐため、文字列内のシングルクォートをエスケープ
    const escaped = String(str).replace(/'/g, "''");
    return `'${escaped}'`;
  };

  const visitDate = escapeString(data.visit_date);
  const startTime = escapeString(data.start_time);
  const numRegular = data.num_regular !== undefined && data.num_regular !== null ? data.num_regular : 'NULL';
  const numStudent = data.num_student !== undefined && data.num_student !== null ? data.num_student : 'NULL';
  const language = escapeString(data.language);

  const query = `
    INSERT INTO
      \`${PROJECT_ID}.${DATASET_TABLE}\`
      (
        reservation_id,
        submission_id,
        submitted_at,
        visit_date,
        start_time,
        name,
        num_regular,
        num_student,
        \`language\`
      )
      VALUES (
        GENERATE_UUID(),
        GENERATE_UUID(),
        CURRENT_TIMESTAMP(),
        ${visitDate}, -- DATE type (YYYY-MM-DD 形式の文字列)
        ${startTime}, -- TIME type (HH:MM:SS 形式の文字列)
        '飛び入り参加',
        ${numRegular}, -- INT64 type
        ${numStudent}, -- INT64 type
        ${language} -- STRING type
      )
  `;
  
  Logger.log("生成された BigQuery クエリ: " + query);
  return query;
}

/**
 * Dateオブジェクトまたは日付/時刻文字列をBigQueryの形式に変換します。
 *
 * @param {string | Date} dateValue フォームの回答値
 * @param {string} format 変換後のフォーマット文字列 (例: "yyyy-MM-dd", "HH:mm:ss")
 * @returns {string} 変換された日付/時刻文字列
 */
function formatDateForBigQuery(dateValue, format) {
  if (!dateValue) return null;

  // 渡された値が既に文字列で、かつ時刻形式 (HH:MM) の変換を求めている場合
  // New Date()に依存せず、直接 'HH:MM:SS' 形式に変換する
  if (typeof dateValue === 'string' && format === "HH:mm:ss") {
    // 例: '13:00' -> '13:00:00'
    const timeParts = dateValue.split(':');
    if (timeParts.length === 2) {
      // 分まである場合、秒として ':00' を追加
      return `${timeParts[0]}:${timeParts[1]}:00`;
    }
    // 時刻がHH:MM形式でない場合はそのまま返すか、エラーとして扱う（ここでは元の値を返す）
    Logger.log('時刻形式の回答が予期せぬ形式です: ' + dateValue);
    return dateValue; 
  }


  let dateObject;
  if (dateValue instanceof Date) {
    dateObject = dateValue;
  } else {
    // 回答が文字列の場合、Dateオブジェクトに変換を試みる
    // 例: '2025/03/29' のような日付文字列
    dateObject = new Date(dateValue);
  }

  // 無効な日付の場合は null を返す
  if (isNaN(dateObject.getTime())) {
    Logger.log('無効な日付/時刻値: ' + dateValue);
    return null;
  }

  // スクリプトのタイムゾーンを使用してフォーマット
  return Utilities.formatDate(dateObject, Session.getScriptTimeZone(), format);
}