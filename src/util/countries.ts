/**
 * ISO 3166-1 alpha-2 → Country name mapping
 * Used across the app to display human-readable country names
 */
export const COUNTRY_NAMES: Record<string, string> = {
  AE: "UAE", AF: "Afghanistan", AL: "Albania", AM: "Armenia", AT: "Austria",
  AU: "Australia", AZ: "Azerbaijan", BD: "Bangladesh", BE: "Belgium",
  BF: "Burkina Faso", BG: "Bulgaria", BR: "Brazil", BW: "Botswana",
  CA: "Canada", CF: "Central African Rep.", CH: "Switzerland", CM: "Cameroon",
  CN: "China", CO: "Colombia", CZ: "Czechia", DE: "Germany", DK: "Denmark",
  EC: "Ecuador", EE: "Estonia", EG: "Egypt", ES: "Spain", ET: "Ethiopia",
  FI: "Finland", FJ: "Fiji", FR: "France", GA: "Gabon", GB: "United Kingdom",
  GH: "Ghana", GR: "Greece", GT: "Guatemala", HK: "Hong Kong", HN: "Honduras",
  HR: "Croatia", HU: "Hungary", ID: "Indonesia", IE: "Ireland", IL: "Israel",
  IN: "India", IR: "Iran", IS: "Iceland", IT: "Italy", JM: "Jamaica",
  JO: "Jordan", JP: "Japan", KE: "Kenya", KR: "South Korea", KZ: "Kazakhstan",
  LK: "Sri Lanka", LT: "Lithuania", LV: "Latvia", MA: "Morocco", MD: "Moldova", MT: "Malta",
  MG: "Madagascar", ML: "Mali", MM: "Myanmar", MN: "Mongolia", MW: "Malawi",
  MX: "Mexico", MY: "Malaysia", MZ: "Mozambique", NG: "Nigeria", NL: "Netherlands",
  NO: "Norway", NP: "Nepal", NZ: "New Zealand", PE: "Peru", PG: "Papua New Guinea",
  PH: "Philippines", PK: "Pakistan", PL: "Poland", PT: "Portugal", RO: "Romania",
  RS: "Serbia", RU: "Russia", RW: "Rwanda", SA: "Saudi Arabia", SE: "Sweden",
  SD: "Sudan", SG: "Singapore", SI: "Slovenia", SK: "Slovakia", SN: "Senegal", SV: "El Salvador",
  TH: "Thailand", TN: "Tunisia", TR: "Turkey", TW: "Taiwan", TZ: "Tanzania",
  UA: "Ukraine", UG: "Uganda", US: "United States", UZ: "Uzbekistan",
  VE: "Venezuela", VN: "Vietnam", ZA: "South Africa", ZM: "Zambia", ZW: "Zimbabwe", AO: "Angola",
  UM: "U.S. Minor Outlying Islands", unknown: "Unknown",
};

/** Chinese country name mapping */
export const COUNTRY_NAMES_ZH: Record<string, string> = {
  AE: "阿联酋", AF: "阿富汗", AL: "阿尔巴尼亚", AM: "亚美尼亚", AT: "奥地利",
  AU: "澳大利亚", AZ: "阿塞拜疆", BD: "孟加拉国", BE: "比利时",
  BF: "布基纳法索", BG: "保加利亚", BR: "巴西", BW: "博茨瓦纳",
  CA: "加拿大", CF: "中非共和国", CH: "瑞士", CM: "喀麦隆",
  CN: "中国", CO: "哥伦比亚", CZ: "捷克", DE: "德国", DK: "丹麦",
  EC: "厄瓜多尔", EE: "爱沙尼亚", EG: "埃及", ES: "西班牙", ET: "埃塞俄比亚",
  FI: "芬兰", FJ: "斐济", FR: "法国", GA: "加蓬", GB: "英国",
  GH: "加纳", GR: "希腊", GT: "危地马拉", HK: "香港", HN: "洪都拉斯",
  HR: "克罗地亚", HU: "匈牙利", ID: "印度尼西亚", IE: "爱尔兰", IL: "以色列",
  IN: "印度", IR: "伊朗", IS: "冰岛", IT: "意大利", JM: "牙买加",
  JO: "约旦", JP: "日本", KE: "肯尼亚", KR: "韩国", KZ: "哈萨克斯坦",
  LK: "斯里兰卡", LT: "立陶宛", LV: "拉脱维亚", MA: "摩洛哥", MD: "摩尔多瓦", MT: "马耳他",
  MG: "马达加斯加", ML: "马里", MM: "缅甸", MN: "蒙古", MW: "马拉维",
  MX: "墨西哥", MY: "马来西亚", MZ: "莫桑比克", NG: "尼日利亚", NL: "荷兰",
  NO: "挪威", NP: "尼泊尔", NZ: "新西兰", PE: "秘鲁", PG: "巴布亚新几内亚",
  PH: "菲律宾", PK: "巴基斯坦", PL: "波兰", PT: "葡萄牙", RO: "罗马尼亚",
  RS: "塞尔维亚", RU: "俄罗斯", RW: "卢旺达", SA: "沙特阿拉伯", SE: "瑞典",
  SG: "新加坡", SI: "斯洛文尼亚", SK: "斯洛伐克", SN: "塞内加尔", SV: "萨尔瓦多",
  TH: "泰国", TN: "突尼斯", TR: "土耳其", TW: "台湾", TZ: "坦桑尼亚",
  UA: "乌克兰", UG: "乌干达", US: "美国", UZ: "乌兹别克斯坦",
  VE: "委内瑞拉", VN: "越南", ZA: "南非", ZM: "赞比亚", ZW: "津巴布韦", AO: "安哥拉",
  SD: "苏丹", UM: "美国本土外小岛屿", unknown: "未知",
};

/** Age group Chinese name mapping */
export const AGE_GROUP_ZH: Record<string, string> = {
  Infant: "婴儿", Child: "儿童", Adolescent: "青少年", Adult: "成人",
  Older_Adult: "老年人", Centenarian: "百岁老人", Oldest_Old: "高龄老人", Unknown: "未知",
};

/** Sex Chinese name mapping */
export const SEX_ZH: Record<string, string> = {
  male: "男", female: "女", unknown: "未知",
};

// Reverse map: English name → ISO code (built once)
const EN_TO_ISO: Record<string, string> = {};
for (const [iso, en] of Object.entries(COUNTRY_NAMES)) EN_TO_ISO[en] = iso;

/** Convert ISO code OR English name to display name */
export function countryName(codeOrName: string, locale?: string): string {
  // Try direct ISO lookup first
  if (COUNTRY_NAMES[codeOrName]) {
    if (locale === "zh") return COUNTRY_NAMES_ZH[codeOrName] ?? COUNTRY_NAMES[codeOrName];
    return COUNTRY_NAMES[codeOrName];
  }
  // Try reverse lookup (English name → ISO → target locale)
  const iso = EN_TO_ISO[codeOrName];
  if (iso && locale === "zh") return COUNTRY_NAMES_ZH[iso] ?? codeOrName;
  return codeOrName;
}
