# region Prediction rules
import numpy as np

TYPE_2_CLASS_MAP = {"operator": "eq"
    , "rules": {
        'Car service': 23  # 'Car expenses'
        , 'Airline': 25  # 'Vacation'
        , 'Supermarket': 20  # 'Groceries'
        , 'Fuel supplier': 17  # 'Gasoline'
        , 'חניון בתל אביב': 17  # 'Gasoline'
        , 'Clothing store': 8  # 'Clothing'
        , 'Transport company': 18  # 'Transport'
        , 'Rock climbing': 6  # 'Climbing'
        , 'Medical clinic': 21  # 'Haircuts, medicine and cosmetics'
        , 'Tire shop': 23  # 'Car expenses'
        , 'Plant nursery': 43  # 'Garden'
    }
                    }

CATEGORY_RAW_2_CLASS_MAP = {"operator": "eq"
    , "rules": {
        'שירותי רכב': 23  # 'Car expenses'
        , 'מוסכים': 23  # 'Car expenses'
        , 'רהיטים': 28  # 'Household'
        , 'צעצועים': 44  # 'Adam expenses'
        , 'פירות וירקות': 20  # 'Groceries'
        , 'פארמה': 21  # 'Haircut and Pharma'
        , 'נופש ותיור': 25  # 'Vacation'
        , 'משתלות': 43  # 'Garden'
        , 'מכולת/סופר': 20  # 'Groceries'
        , 'מינימרקטים ומכולות': 20  # 'Groceries'
        , 'הלבשה': 8  # 'Clothing'
        , 'דלק': 17  # 'Gasoline'
    }
                            }

NAME_2_CLASS_MAP = {
    "operator": "contains"
    , "rules": {
        'כספומט': 19  # 'Cash'
        , 'paybox': np.nan
        , 'פנגו': 18  # 'Transport'
        , 'BUBBLE DAN': 18  # 'Transport'
        , 'העברה באפליקציית box': np.nan
        , 'העברה ב bit בנה"פ': np.nan
        , 'ישראכרט': np.nan
    }
}

# Classes to dismiss prediction because they are low precision
CLASS_2_CLASS_MAP = {
    "operator": "eq"
    , "rules": {
        np.nan: np.nan,
        None: np.nan,
        46: np.nan  # Presents received
        , 45: np.nan  # New apartment
        , 43: np.nan  # Garden
        , 42: np.nan  # Eden's work expenses
        , 41: np.nan  # IGNORE
        , 40: np.nan  # Noam's paycheck
        , 38: np.nan  # HaOgen (house holding)
        , 32: np.nan  # Car test
        , 23: np.nan  # Car expenses
        , 31: np.nan  # Accountant US taxes
        , 30: np.nan  # Events & Concerts
        , 27: np.nan  # Weddings
        , 26: np.nan  # Dental
        , 16: np.nan  # Mutual leisure
        , 11: np.nan  # Startup expenses
        , 10: np.nan  # Personal leisure
        , 8: np.nan  # Clothing
        , 7: np.nan  # Goalball
        , 29: np.nan  # Goodies

    }
}
# endregion

# region Preprocessing Rules (Org. types, tokens etc.)
SWITCH_TERMS = {
    "coffe": "קפה"
    , "paypal ebay": "ebay"
    , "paypal  grammarly": "grammarly"
    , "paypal  steam games": "steam games"
    , "paypal  spotify": "spotify"
    , "paypal  booking": "booking"
    , 'ת"א -יפו': 'ת״א'
    , 'ת"א יפו': 'ת״א'
    , "תא": 'ת״א'
    , "ת א": 'ת״א'
    , "חניון": 'חניה'
    , "תל אביב": 'ת״א'
    , "כספונט": 'כספומט'
    , "בנקט": 'כספומט'
    , "פירות וירקות": 'פירות'
    , "a i g": 'aig'
    , "דרך ארץ הוראת קבע": 'כביש 6'
}
BRANDS = [
    'audible'
    , 'פז yellow'
    , 'סיבוס'
    , 'קסטרו'
    , 'הום סנטר'
    , 'משיכת שיק'
    , 'כספומט'
    , 'ישראכרט'
    , 'סונול'
    , 'שילב'
    , 'דלק מנטה'
    , 'רולדין'
    , 'צומת ספרים'
    , 'איכילוב'
    , 'פנגו'
    , 'ארומה'
    , 'איקאה'
    , 'כללית'
    , 'דלק'
    , 'חניה'
    , 'חניון'
    , 'חומוס'
    , 'spotify'
    , 'booking'
    , 'שאוורמה'
    , 'קפה'
    , 'פיצוחי'
    , 'פיצה'
    , 'מסעדת'
    , 'מסעדה'
    , 'שוק'
    , 'שווארמה'
    , 'מילואים'
    , 'גולדה'
    , 'רכבת ישראל'
    , 'סופר פארם'
    , 'שופרסל'
    , 'סופר'
]
STOPWORDS = ["בע״מ"
    , "בעמ"
    , "בע''מ"
    , 'בע"מ'
    , 'בע"'
    , "בע'"
    , "בע''"
    , 'אנד'
    , "סניף"
    , "com"
    , "www"
    , '-גמא'
             ]
TYPE_NORMALIZATIONS = {
    "restaurant": "Restaurant"
    , "מסעדה": "Restaurant"
    , "מסעדת": "Restaurant"
    , "bar": "Restaurant"
    , "בית קפה": "Restaurant"
}
NAME_2_TYPE_RULES = {
    'אי.אם.פי.אם': 'Supermarket',
    'מוסכי': 'Car service',
    ' lim ride ': 'Transport',
    'מוסך': 'Car service',
    'מכון רישוי': 'Car service',
    'רשיונות': 'Car service',
    'חומוס': 'Restaurant',
    'קפה': 'Restaurant',
    'דלק': 'Gasoline',
    'פנגו': 'Transport',
    'מרכז לטיפוס סלעים בתל אביב': 'Climbing',
    'צמחים': 'Plant nursery'
}
# endregion
