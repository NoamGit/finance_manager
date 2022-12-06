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
    'מרכז לטיפוס סלעים בתל אביב': 'Climbing',
    'צמחים': 'Plant nursery'
}
TYPE_NORMALIZATIONS = {
    "restaurant": "Restaurant"
    , "מסעדה": "Restaurant"
    , "מסעדת": "Restaurant"
    , "bar": "Restaurant"
    , "בית קפה": "Restaurant"
}
TYPE_2_CLASS_MAP = {
    'Car service': 'Car expenses'
    , 'Airline': 'Vacation'
    , 'Supermarket': 'Groceries'
    , 'Fuel supplier': 'Gasoline'
    , 'חניון בתל אביב': 'Gasoline'
    , 'Clothing store': 'Clothing'
    , 'Rock climbing': 'Climbing'
    , 'Medical clinic': 'Haircuts, medicine and cosmetics'
    , 'Tire shop': 'Car expenses'
    , 'Plant nursery': 'Garden'
}
NAME_2_CLASS_MAP = {
    'כספומט': 'Cash'
    , 'paybox': None
    , 'העברה באפליקציית box': None
    , 'העברה ב bit בנה"פ': None
    , 'ישראכרט': None
}
