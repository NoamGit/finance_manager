from enum import Enum

import pandas as pd


class HighLevelCategories(Enum):
    VARIABLE_EXPENSE_NOAM = "🧔 הוצאות משתנות (נעם)"
    VARIABLE_EXPENSE_EDEN = "👩 הוצאות משתנות (עדן)"
    VARIABLE_EXPENSE_MUTUAL = "👨‍👩‍👧‍👦 הוצאות משתנות (משותף)"
    TRANSPORTATION = "🚙 הוצאות דלק ותחבורה"
    GROCERIES = "🥕 הוצאות סופר 🍏"
    FIXED = "🏠 הוצאות קבועות"
    VARIABLE = "הוצאות משתנות"


THEME_CATEGORY = \
    {
        20: HighLevelCategories.GROCERIES.value,
        16: HighLevelCategories.VARIABLE_EXPENSE_MUTUAL.value,
        21: HighLevelCategories.GROCERIES.value,
        17: HighLevelCategories.TRANSPORTATION.value,
        18: HighLevelCategories.TRANSPORTATION.value,
        33: HighLevelCategories.FIXED.value,
        34: HighLevelCategories.FIXED.value,
        35: HighLevelCategories.FIXED.value,
        36: HighLevelCategories.FIXED.value,
        37: HighLevelCategories.FIXED.value,
        38: HighLevelCategories.FIXED.value,
        44: HighLevelCategories.FIXED.value,
        9: HighLevelCategories.FIXED.value,
        22: HighLevelCategories.FIXED.value,
        24: HighLevelCategories.FIXED.value,
    }


def categorize_by_account_number(s: pd.Series) -> str:
    if s["account_number"] == "1029" and s["high_category"] == "הוצאות משתנות":
        return HighLevelCategories.VARIABLE_EXPENSE_NOAM.value
    elif s["account_number"] == "5094" and s["high_category"] == "הוצאות משתנות":
        return HighLevelCategories.VARIABLE_EXPENSE_EDEN.value
    elif s["high_category"] == "הוצאות משתנות":
        return HighLevelCategories.VARIABLE_EXPENSE_MUTUAL.value
    else:
        return s["high_category"]
