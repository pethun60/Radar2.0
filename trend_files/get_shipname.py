import re

# List of filenames
filenames = [
    'TREND FI_Kauhavan_Lampo.gz',
    'TREND Marine Norrkoping_Azura.gz',
    'TREND Marine Norrkoping_Conquest.gz',
    'TREND Marine Norrkoping_Emerald_P.gz',
    'TREND Marine Norrkoping_Liberty.gz',
    'TREND Marine Norrkoping_Miracle2.gz',
    'TREND Marine Norrkoping_Radiance.gz',
    'TREND Marine Norrkoping_Regal.gz',
    'TREND Marine Norrkoping_Spirit_2.gz',
    'TREND Marine Norrkoping_Sunshine.gz',
    'TREND Marine_Norrkoping_Bliss_FC.gz',
    'TREND Marine_Norrkoping_Breakaway.gz',
    'TREND Marine_Norrkoping_Encore_FC.gz',
    'TREND Marine_Norrkoping_Escape.gz',
    'TREND Marine_Norrkoping_Joy_AC.gz',
    'TREND Marine_Norrkoping_Joy_FC.gz',
    'TREND Marine_Norrkoping_Ruby_Princess.gz',
    'TREND Marine_Norrkoping_Venezia_AC.gz',
    'TREND Marine_Nrk_Spectrum_1.gz',
    'TREND Trend_Marine_Norrkoping_Bliss.gz'
]

# Regular expression to extract the ship name
ship_names = []
# pattern = re.compile(r'_(.+?)\.gz$')
#pattern = re.compile(r'(?<=Norrkoping_)(.+?)(?=\.gz$)|(?<=_Norrkoping_)(.+?)(?=\.gz$)')
pattern = re.compile(r'(?<=Norrkoping_)(.+?)(?=\.gz$)|(?<=_Norrkoping_)(.+?)(?=\.gz$)|(?<=Marine_Nrk_)(.+?)(?=\.gz$)')


for filename in filenames:
    match = pattern.search(filename)
    if match:
        #ship_name = match.group(1)  # Get the ship name from the regex group
        ship_name = next(filter(None, match.groups()))
        ship_names.append(ship_name)

# Print the extracted ship names
for name in ship_names:
    print(name)