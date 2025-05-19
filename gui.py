# gui.py
import os
import PySimpleGUI as sg
from processing import preparator, converter

sg.theme('SystemDefault')

layout = [
    [sg.Text('1. Glisser-déposer la samplesheet CSV :')],
    [sg.Input(key='-IN-', enable_events=True, visible=False),
     sg.FileBrowse(button_text='📂 Choisir ou déposer ici', key='-FILE-',
                   file_types=(("CSV Files", "*.csv"),))],
    [sg.Text('Fichier sélectionné :'), sg.Text('', key='-FILEPATH-')],
    [sg.HorizontalSeparator()],
    [sg.Text('2. Choisir le dossier de sortie :')],
    [sg.In(key='-OUT-', enable_events=True, visible=False),
     sg.FolderBrowse(button_text='📁 Sélectionner dossier', key='-FOLDER-')],
    [sg.Text('Dossier sélectionné :'), sg.Text('', key='-FOLDERPATH-')],
    [sg.HorizontalSeparator()],
    [sg.Checkbox('Générer aussi les fichiers convertis (étape 2)', default=True, key='-DO_CONV-')],
    [sg.Button('Lancer', key='-RUN-', size=(10,1)), sg.Button('Quitter')],
    [sg.Multiline(size=(80, 10), key='-LOG-', autoscroll=True, disabled=True)]
]

window = sg.Window('Auto SampleSheet GUI', layout, finalize=True)

while True:
    event, values = window.read()
    if event in (sg.WIN_CLOSED, 'Quitter'):
        break

    if event == '-FILE-':
        window['-FILEPATH-'].update(values['-FILE-'] or '')
    if event == '-FOLDER-':
        window['-FOLDERPATH-'].update(values['-FOLDER-'] or '')

    if event == '-RUN-':
        inp = values['-FILE-']
        outd = values['-FOLDER-']
        do_conv = values['-DO_CONV-']
        window['-LOG-'].update('')  # clear log

        if not inp or not os.path.isfile(inp):
            sg.popup_error('Veuillez sélectionner un fichier CSV valide.')
            continue
        if not outd or not os.path.isdir(outd):
            sg.popup_error('Veuillez sélectionner un dossier de sortie valide.')
            continue

        # Étape 1 : preparator
        window['-LOG-'].print(f"[Preparator] → {os.path.basename(inp)}")
        try:
            treated = preparator(inp, outd)
            if treated is None:
                window['-LOG-'].print("❌ Erreur lors du traitement preparator.")
                continue
            window['-LOG-'].print(f"✅ Fichier traité : {treated}")
        except Exception as e:
            window['-LOG-'].print(f"❌ Exception: {e}")
            continue

        # Étape 2 : converter (optionnelle)
        if do_conv:
            window['-LOG-'].print("[Converter] → génération des sous-fichiers CSV")
            try:
                conv_files = converter(
                    treated,
                    os.path.join(outd, os.path.splitext(os.path.basename(treated))[0]),
                    delimiter=',', rc_i5=False, no_header=True
                )
                for f in conv_files:
                    window['-LOG-'].print(f"   • {f}")
                window['-LOG-'].print("✅ Conversion terminée.")
            except Exception as e:
                window['-LOG-'].print(f"❌ Exception converter: {e}")

        sg.popup_ok('Traitement terminé !')

window.close()
