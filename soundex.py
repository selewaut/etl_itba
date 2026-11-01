def soundex(name: str) -> str:
    """Return the Soundex code for a given string (classic American Soundex)."""
    if not name:
        return ""
    
    name = name.upper()
    # Keep only letters
    name = ''.join(c for c in name if c.isalpha())
    if not name:
        return ""
    
    # Mapping of consonants to digits
    mapping = {
        'B': '1', 'F': '1', 'P': '1', 'V': '1',
        'C': '2', 'G': '2', 'J': '2', 'K': '2', 'Q': '2', 'S': '2', 'X': '2', 'Z': '2',
        'D': '3', 'T': '3',
        'L': '4',
        'M': '5', 'N': '5',
        'R': '6'
    }
    
    # First letter is kept as-is
    first = name[0]
    code = first
    
    # Process the rest of the letters
    prev_digit = mapping.get(first, '0')  # '0' means non-coded (vowel/H/W/Y)
    
    for char in name[1:]:
        digit = mapping.get(char, '0')
        # Skip if non-coded or same as previous coded digit
        if digit != '0' and digit != prev_digit:
            code += digit
        # Update previous only if it was a coded letter
        # (H and W are "transparent" in classic Soundex, but this simple version
        # treats them as separators which is close enough for most cases)
        if digit != '0':
            prev_digit = digit
        elif char not in 'HW':
            prev_digit = '0'
    
    # Pad with zeros or truncate to 4 characters total
    code = code.ljust(4, '0')[:4]
    return code


# --- Tests ---
pairs = [
    ("Smith", "Smyth"),
    ("Knight", "Night"),
    ("Robert", "Rupert"),
    ("Ashcraft", "Ashcroft"),
    ("Pfister", "Fister"),
]

for a, b in pairs:
    print(f"{a:10} → {soundex(a)}")
    print(f"{b:10} → {soundex(b)}")
    print(f"Match: {soundex(a) == soundex(b)}\n")