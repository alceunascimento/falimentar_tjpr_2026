#!/usr/bin/env python3
"""
Consolidated PROJUDI Data Processor
===================================

Sistema integrado para consulta, processamento e agregação de dados processuais
via webservice SOAP do PROJUDI/TJPR.

Funcionalidades:
- Consulta SOAP de processos judiciais
- Processamento e organização de dados XML/JSON
- Extração e agregação de dados resumidos
- Exportação para formato CSV estruturado

Usage:
    python script.py {id_consultante} {codigo_privado} --csv {arquivo.csv}

Autores: Sistema Automatizado de Processamento Judicial
Data: 2025
Licença: Uso restrito para fins acadêmicos e de pesquisa
"""

import hashlib
import requests
import xml.etree.ElementTree as ET
import json
import os
import sys
import csv
import time
import logging
import warnings
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from tqdm import tqdm
from urllib3.exceptions import InsecureRequestWarning

# Configuração de avisos e logging
warnings.simplefilter('ignore', InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler('projudi_processing.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Constantes do sistema
SOAP_URL = 'https://projudi.tjpr.jus.br/projudi_consulta/webservices/projudiIntercomunicacaoWebService222'
SOAP_ACTION = '"http://www.cnj.jus.br/servico-intercomunicacao-2.2.2/consultarProcesso"'
XML_NAMESPACES = {
    'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
    'ns4': 'http://www.cnj.jus.br/servico-intercomunicacao-2.2.2/',
    'ns2': 'http://www.cnj.jus.br/intercomunicacao-2.2.2',
    'tipos': 'http://www.cnj.jus.br/tipos-servico-intercomunicacao-2.2.2'
}

class ProcessamentoError(Exception):
    """Exceção personalizada para erros de processamento."""
    pass

class SOAPClientError(Exception):
    """Exceção personalizada para erros de comunicação SOAP."""
    pass

class PROJUDIProcessor:
    """
    Processador integrado para dados do sistema PROJUDI.
    
    Implementa pipeline completo de ETL (Extract, Transform, Load) para
    dados processuais obtidos via webservice SOAP.
    """
    
    def __init__(self, id_consultante: str, codigo_privado: str, output_dir: str = "data_mni"):
        """
        Inicializa o processador PROJUDI.
        
        Args:
            id_consultante: Identificador do consultante no sistema
            codigo_privado: Código privado para autenticação
            output_dir: Diretório base para armazenamento de dados
        """
        self.id_consultante = id_consultante
        self.codigo_privado = codigo_privado
        self.output_dir = Path(output_dir)
        
        # Criação de diretórios estruturados
        self.raw_dir = self.output_dir / "raw"
        self.processed_dir = self.output_dir / "processed"
        self.aggregated_dir = self.output_dir / "aggregated"
        
        self._create_directories()
        
        # Estruturas de dados para agregação
        self.processed_data: List[Dict[str, Any]] = []
        self.all_fields: set = set()
        self.error_files: List[str] = []
        self.processing_stats = {
            'total_processes': 0,
            'successful_downloads': 0,
            'successful_processing': 0,
            'errors': 0,
            'start_time': None,
            'end_time': None
        }
        
        logger.info(f"PROJUDI Processor inicializado")
        logger.info(f"ID Consultante: {self.id_consultante}")
        logger.info(f"Diretório de saída: {self.output_dir}")
    
    def _create_directories(self) -> None:
        """Cria estrutura de diretórios necessária."""
        for directory in [self.raw_dir, self.processed_dir, self.aggregated_dir]:
            directory.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Diretório criado/verificado: {directory}")
    
    def _generate_auth_password(self) -> str:
        """
        Gera senha MD5 para autenticação usando código privado + data atual.
        
        Returns:
            String MD5 hash para autenticação
        """
        today = datetime.now().strftime("%Y%m%d")
        password_md5 = hashlib.md5((self.codigo_privado + today).encode('utf-8')).hexdigest()
        logger.debug(f"Senha de autenticação gerada para data: {today}")
        return password_md5
    
    def _generate_soap_request(self, numero_processo: str) -> str:
        """
        Gera XML de solicitação SOAP conforme especificação CNJ.
        
        Args:
            numero_processo: Número do processo a ser consultado
            
        Returns:
            String XML da solicitação SOAP
        """
        senha_consultante = self._generate_auth_password()
        
        xml_template = """<?xml version="1.0" encoding="UTF-8"?>
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                          xmlns:ser="http://www.cnj.jus.br/servico-intercomunicacao-2.2.2/"
                          xmlns:tip="http://www.cnj.jus.br/tipos-servico-intercomunicacao-2.2.2">
           <soapenv:Header/>
           <soapenv:Body>
              <ser:consultarProcesso>
                 <tip:idConsultante>{id_consultante}</tip:idConsultante>
                 <tip:senhaConsultante>{senha_consultante}</tip:senhaConsultante>
                 <tip:numeroProcesso>{numero_processo}</tip:numeroProcesso>
                 <tip:movimentos>true</tip:movimentos>
                 <tip:incluirCabecalho>true</tip:incluirCabecalho>
                 <tip:incluirDocumentos>false</tip:incluirDocumentos>
                 <tip:documento></tip:documento>
              </ser:consultarProcesso>
           </soapenv:Body>
        </soapenv:Envelope>"""
        
        return xml_template.format(
            id_consultante=self.id_consultante,
            senha_consultante=senha_consultante,
            numero_processo=numero_processo
        )
    
    def _execute_soap_request(self, numero_processo: str, max_retries: int = 3) -> bytes:
        """
        Executa requisição SOAP com retry automático e backoff exponencial.
        
        Args:
            numero_processo: Número do processo
            max_retries: Número máximo de tentativas
            
        Returns:
            Conteúdo XML da resposta
            
        Raises:
            SOAPClientError: Em caso de falha na comunicação
        """
        xml_request = self._generate_soap_request(numero_processo)
        headers = {
            'Content-Type': 'text/xml;charset=UTF-8',
            'SOAPAction': SOAP_ACTION,
            'User-Agent': 'PROJUDI-Data-Processor/1.0'
        }
        
        retry_delay = 2  # segundos
        
        for attempt in range(max_retries):
            try:
                logger.debug(f"Tentativa {attempt + 1}/{max_retries} para processo {numero_processo}")
                
                response = requests.post(
                    url=SOAP_URL,
                    data=xml_request.encode('utf-8'),
                    headers=headers,
                    verify=False,
                    timeout=30
                )
                
                if response.status_code == 200:
                    logger.info(f"Processo {numero_processo}: Sucesso (HTTP {response.status_code})")
                    return response.content
                else:
                    logger.warning(f"Processo {numero_processo}: HTTP {response.status_code}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue
                    else:
                        raise SOAPClientError(f"HTTP {response.status_code}: {response.text[:200]}")
                        
            except requests.exceptions.RequestException as e:
                logger.error(f"Erro de conexão para processo {numero_processo}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    raise SOAPClientError(f"Falha na conexão após {max_retries} tentativas: {e}")
    
    def _save_raw_response(self, xml_response: bytes, numero_processo: str) -> Path:
        """
        Armazena resposta XML bruta em formato JSON estruturado.
        
        Args:
            xml_response: Resposta XML do webservice
            numero_processo: Número do processo
            
        Returns:
            Caminho do arquivo salvo
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{numero_processo}_raw_{timestamp}.json"
        filepath = self.raw_dir / filename
        
        raw_data = {
            'numero_processo': numero_processo,
            'timestamp': timestamp,
            'timestamp_iso': datetime.now().isoformat(),
            'resposta_raw': xml_response.decode('utf-8'),
            'metadata': {
                'file_size_bytes': len(xml_response),
                'encoding': 'utf-8',
                'source': 'PROJUDI-TJPR'
            }
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=2)
        
        logger.debug(f"Resposta raw salva: {filepath}")
        return filepath
    
    @staticmethod
    def _format_datetime(date_str: str) -> Optional[str]:
        """
        Formata string de data do formato AAAAMMDDHHMMSS para DD/MM/AAAA HH:MM:SS.
        
        Args:
            date_str: String de data no formato compacto
            
        Returns:
            String formatada ou None se inválida
        """
        if not date_str or len(date_str) != 14:
            return date_str
        try:
            return f"{date_str[6:8]}/{date_str[4:6]}/{date_str[0:4]} {date_str[8:10]}:{date_str[10:12]}:{date_str[12:14]}"
        except (ValueError, IndexError):
            return date_str
    
    def _extract_codigos_nacionais(self, json_data: Dict[str, Any]) -> List[str]:
        """
        Extrai códigos nacionais da estrutura processo.dados_basicos.assuntos.
        
        Args:
            json_data: Dados processados do JSON
            
        Returns:
            Lista de códigos nacionais identificados
        """
        codigos = []
        try:
            processo = json_data.get('processo', {})
            dados_basicos = processo.get('dados_basicos', {})
            assuntos = dados_basicos.get('assuntos', [])
            
            if isinstance(assuntos, list):
                for assunto in assuntos:
                    if isinstance(assunto, dict):
                        codigo = assunto.get('codigoNacional')
                        if codigo and str(codigo).strip():
                            codigos.append(str(codigo).strip())
                            
        except Exception as e:
            logger.debug(f"Erro na extração de códigos nacionais: {e}")
        
        return codigos
    
    def _organize_raw_response(self, raw_filepath: Path) -> Dict[str, Any]:
        """
        Organiza resposta XML bruta em estrutura hierárquica JSON.
        
        Args:
            raw_filepath: Caminho do arquivo de resposta bruta
            
        Returns:
            Dicionário com dados organizados
            
        Raises:
            ProcessamentoError: Em caso de falha no processamento
        """
        try:
            with open(raw_filepath, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
            
            # Parse do XML
            root = ET.fromstring(raw_data['resposta_raw'])
            
            # Estrutura de saída organizada
            organized_data = {
                'metadados': {
                    'numero_processo': raw_data['numero_processo'],
                    'data_consulta': raw_data['timestamp'],
                    'data_formatada': datetime.strptime(
                        raw_data['timestamp'], "%Y%m%d_%H%M%S"
                    ).strftime("%d/%m/%Y %H:%M:%S"),
                    'arquivo_origem': raw_filepath.name
                },
                'resposta': {
                    'sucesso': None,
                    'mensagem': None
                },
                'processo': {
                    'dados_basicos': {},
                    'movimentos': []
                }
            }
            
            # Extração de informações de resposta
            resposta_elem = root.find('.//ns4:consultarProcessoResposta', XML_NAMESPACES)
            if resposta_elem is not None:
                sucesso_elem = resposta_elem.find('./sucesso')
                mensagem_elem = resposta_elem.find('./mensagem')
                
                if sucesso_elem is not None:
                    organized_data['resposta']['sucesso'] = (sucesso_elem.text.lower() == 'true')
                
                if mensagem_elem is not None:
                    organized_data['resposta']['mensagem'] = mensagem_elem.text
            
            # Extração de dados básicos do processo
            dados_basicos_elem = root.find('.//ns2:dadosBasicos', XML_NAMESPACES)
            if dados_basicos_elem is not None:
                self._extract_dados_basicos(dados_basicos_elem, organized_data)
            
            # Extração de movimentos processuais
            self._extract_movimentos(root, organized_data)
            
            # Geração de dados resumidos
            self._generate_resumo(organized_data)
            
            return organized_data
            
        except Exception as e:
            raise ProcessamentoError(f"Falha no processamento de {raw_filepath}: {e}")
    
    def _extract_dados_basicos(self, dados_elem: ET.Element, organized_data: Dict[str, Any]) -> None:
        """Extrai dados básicos do processo do elemento XML."""
        dados_basicos = organized_data['processo']['dados_basicos']
        
        # Atributos diretos
        for attr, valor in dados_elem.attrib.items():
            dados_basicos[attr] = valor
        
        # Extração de assuntos
        assuntos = []
        for assunto_elem in dados_elem.findall('./ns2:assunto', XML_NAMESPACES):
            assunto = {'atributos': dict(assunto_elem.attrib)}
            
            codigo_nacional = assunto_elem.find('./ns2:codigoNacional', XML_NAMESPACES)
            if codigo_nacional is not None:
                assunto['codigoNacional'] = codigo_nacional.text
            
            assuntos.append(assunto)
        
        if assuntos:
            dados_basicos['assuntos'] = assuntos
        
        # Outros parâmetros
        outros_parametros = []
        for param_elem in dados_elem.findall('./ns2:outroParametro', XML_NAMESPACES):
            param = dict(param_elem.attrib)
            
            # Processamento especial para ARVORE_PROCESSUAL
            if param.get('nome') == 'ARVORE_PROCESSUAL':
                try:
                    import html
                    valor_decodificado = html.unescape(param.get('valor', ''))
                    param['valor_estruturado'] = json.loads(valor_decodificado)
                except (json.JSONDecodeError, Exception):
                    pass
            
            outros_parametros.append(param)
        
        if outros_parametros:
            dados_basicos['outrosParametros'] = outros_parametros
        
        # Elementos simples
        for elem in dados_elem:
            tag_name = elem.tag.split('}')[-1]
            
            if tag_name in ['assunto', 'outroParametro']:
                continue
            elif tag_name == 'orgaoJulgador':
                dados_basicos['orgaoJulgador'] = {'atributos': dict(elem.attrib)}
            else:
                dados_basicos[tag_name] = elem.text
    
    def _extract_movimentos(self, root: ET.Element, organized_data: Dict[str, Any]) -> None:
        """Extrai movimentos processuais do XML."""
        movimentos = []
        
        for mov_elem in root.findall('.//ns2:movimento', XML_NAMESPACES):
            movimento = {
                'atributos': dict(mov_elem.attrib),
                'complementos': [],
                'movimentoNacional': None
            }
            
            # Formatação de dataHora
            data_hora = movimento['atributos'].get('dataHora')
            if data_hora and len(data_hora) == 14:
                movimento['dataHoraFormatada'] = self._format_datetime(data_hora)
            
            # Complementos
            for comp_elem in mov_elem.findall('./ns2:complemento', XML_NAMESPACES):
                if comp_elem.text:
                    movimento['complementos'].append(comp_elem.text)
            
            # Movimento nacional
            mov_nac_elem = mov_elem.find('./ns2:movimentoNacional', XML_NAMESPACES)
            if mov_nac_elem is not None:
                mov_nacional = {
                    'atributos': dict(mov_nac_elem.attrib),
                    'complementos': []
                }
                
                for comp_elem in mov_nac_elem.findall('./ns2:complemento', XML_NAMESPACES):
                    if comp_elem.text:
                        mov_nacional['complementos'].append(comp_elem.text)
                
                movimento['movimentoNacional'] = mov_nacional
            
            movimentos.append(movimento)
        
        # Ordenação por identificador
        movimentos.sort(
            key=lambda x: int(x['atributos'].get('identificadorMovimento', 0))
        )
        
        organized_data['processo']['movimentos'] = movimentos
    
    def _generate_resumo(self, organized_data: Dict[str, Any]) -> None:
        """Gera dados resumidos do processo."""
        dados_basicos = organized_data['processo']['dados_basicos']
        movimentos = organized_data['processo']['movimentos']
        
        # Busca por parâmetros específicos
        outros_params = dados_basicos.get('outrosParametros', [])
        param_dict = {p.get('nome'): p.get('valor') for p in outros_params}
        
        resumo = {
            'numero_processo': organized_data['metadados']['numero_processo'],
            'classe_processual': dados_basicos.get('classeProcessual'),
            'data_ajuizamento': self._format_datetime(dados_basicos.get('dataAjuizamento')),
            'valor_causa': dados_basicos.get('valorCausa'),
            'orgao_julgador': dados_basicos.get('orgaoJulgador', {}).get('atributos', {}).get('nomeOrgao'),
            'situacao': param_dict.get('situacao'),
            'status': param_dict.get('status'),
            'total_movimentos': len(movimentos),
            'ultimo_movimento': {}
        }
        
        # Último movimento
        if movimentos:
            ultimo = movimentos[-1]
            resumo['ultimo_movimento'] = {
                'data': self._format_datetime(ultimo['atributos'].get('dataHora')),
                'descricao': ultimo['complementos'][0] if ultimo['complementos'] else None
            }
        
        organized_data['resumo'] = resumo
    
    def _save_organized_data(self, organized_data: Dict[str, Any], numero_processo: str) -> Path:
        """Salva dados organizados em arquivo JSON."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{numero_processo}_organized_{timestamp}.json"
        filepath = self.processed_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(organized_data, f, ensure_ascii=False, indent=2)
        
        logger.debug(f"Dados organizados salvos: {filepath}")
        return filepath
    
    def _extract_resumo_data(self, organized_data: Dict[str, Any], filename: str) -> Optional[Dict[str, Any]]:
        """
        Extrai dados do resumo para agregação CSV.
        
        Args:
            organized_data: Dados organizados do processo
            filename: Nome do arquivo para referência
            
        Returns:
            Dicionário com dados extraídos para CSV
        """
        try:
            resumo = organized_data.get('resumo')
            if not resumo:
                logger.warning(f"Campo 'resumo' não encontrado em {filename}")
                return None
            
            # Registro processado com metadados
            processed_record = {
                '_arquivo_origem': filename,
                '_timestamp_processamento': datetime.now().isoformat()
            }
            
            # Processamento de campos do resumo
            if isinstance(resumo, dict):
                for key, value in resumo.items():
                    if isinstance(value, dict):
                        # Campos aninhados (ex: ultimo_movimento)
                        for nested_key, nested_value in value.items():
                            field_name = f"{key}_{nested_key}"
                            processed_record[field_name] = nested_value
                            self.all_fields.add(field_name)
                    else:
                        processed_record[key] = value
                        self.all_fields.add(key)
            
            # Códigos nacionais
            codigos_nacionais = self._extract_codigos_nacionais(organized_data)
            processed_record['codigos_nacionais'] = ', '.join(codigos_nacionais) if codigos_nacionais else ''
            processed_record['total_codigos_nacionais'] = len(codigos_nacionais)
            
            # Atualização de campos
            self.all_fields.update([
                '_arquivo_origem', 
                '_timestamp_processamento',
                'codigos_nacionais',
                'total_codigos_nacionais'
            ])
            
            # Conversão de valor_causa para formato brasileiro (. → ,)
            if 'valor_causa' in processed_record and processed_record['valor_causa']:
                processed_record['valor_causa'] = str(processed_record['valor_causa']).replace('.', ',')
            
            return processed_record
            
        except Exception as e:
            logger.error(f"Erro na extração de resumo de {filename}: {e}")
            return None
    
    def _read_csv_processes(self, csv_filepath: str) -> List[str]:
        """
        Lê números de processo de arquivo CSV.
        
        Args:
            csv_filepath: Caminho do arquivo CSV
            
        Returns:
            Lista de números de processo
            
        Raises:
            ProcessamentoError: Em caso de erro na leitura
        """
        try:
            processes = []
            with open(csv_filepath, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                
                # Detecção automática de cabeçalho
                first_line = next(reader, None)
                if first_line and not (first_line[0].isdigit() or first_line[0].startswith('0')):
                    logger.info("Cabeçalho detectado no CSV - primeira linha ignorada")
                else:
                    f.seek(0)
                    reader = csv.reader(f)
                
                # Leitura de processos
                for row_num, row in enumerate(reader, 1):
                    if row and row[0].strip():
                        numero_processo = row[0].strip()
                        if numero_processo:
                            processes.append(numero_processo)
                        else:
                            logger.warning(f"Linha {row_num}: processo vazio ignorado")
            
            logger.info(f"Lidos {len(processes)} processos do arquivo CSV")
            return processes
            
        except Exception as e:
            raise ProcessamentoError(f"Erro na leitura do CSV {csv_filepath}: {e}")
    
    def _write_aggregated_csv(self, output_filepath: str) -> None:
        """
        Escreve dados agregados em arquivo CSV estruturado.
        
        Args:
            output_filepath: Caminho do arquivo CSV de saída
        """
        if not self.processed_data:
            raise ProcessamentoError("Nenhum dado processado disponível para exportação CSV")
        
        # Ordenação de campos: metadados, códigos, alfabético
        meta_fields = ['_arquivo_origem', '_timestamp_processamento']
        codigo_fields = ['codigos_nacionais', 'total_codigos_nacionais']
        other_fields = sorted([f for f in self.all_fields if f not in meta_fields + codigo_fields])
        ordered_fields = meta_fields + codigo_fields + other_fields
        
        try:
            with open(output_filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(
                    csvfile,
                    fieldnames=ordered_fields,
                    extrasaction='ignore'
                )
                
                writer.writeheader()
                
                # Escrita com barra de progresso
                for record in tqdm(self.processed_data, desc="Exportando CSV", unit="registro"):
                    complete_record = {field: record.get(field, '') for field in ordered_fields}
                    writer.writerow(complete_record)
            
            logger.info(f"CSV agregado criado: {output_filepath}")
            logger.info(f"Colunas: {len(ordered_fields)}, Linhas: {len(self.processed_data)}")
            
        except Exception as e:
            raise ProcessamentoError(f"Erro na criação do CSV: {e}")
    
    def _generate_comprehensive_report(self) -> None:
        """Gera relatório abrangente do processamento."""
        if not self.processed_data:
            logger.warning("Dados insuficientes para geração de relatório")
            return
        
        stats = self.processing_stats
        total_time = (stats['end_time'] - stats['start_time']).total_seconds() if stats['end_time'] and stats['start_time'] else 0
        
        logger.info("\n" + "="*60)
        logger.info("RELATÓRIO DE PROCESSAMENTO PROJUDI")
        logger.info("="*60)
        logger.info(f"Tempo total de execução: {total_time:.2f} segundos")
        logger.info(f"Processos totais: {stats['total_processes']}")
        logger.info(f"Downloads bem-sucedidos: {stats['successful_downloads']}")
        logger.info(f"Processamentos bem-sucedidos: {stats['successful_processing']}")
        logger.info(f"Erros: {stats['errors']}")
        
        if stats['total_processes'] > 0:
            success_rate = (stats['successful_processing'] / stats['total_processes']) * 100
            logger.info(f"Taxa de sucesso: {success_rate:.2f}%")
            
            if total_time > 0:
                throughput = stats['successful_processing'] / total_time
                logger.info(f"Throughput: {throughput:.2f} processos/segundo")
        
        # Análise de códigos nacionais
        self._analyze_codigos_nacionais()
        
        # Análise de campos
        self._analyze_field_frequency()
        
        logger.info("="*60)
    
    def _analyze_codigos_nacionais(self) -> None:
        """Analisa distribuição de códigos nacionais."""
        logger.info(f"\nAnálise de Códigos Nacionais:")
        
        codigos_stats = {
            'com_codigos': 0,
            'sem_codigos': 0,
            'codigos_unicos': set()
        }
        
        codigo_frequency = {}
        
        for record in self.processed_data:
            codigos_str = record.get('codigos_nacionais', '')
            if codigos_str:
                codigos_stats['com_codigos'] += 1
                codigos_list = [c.strip() for c in codigos_str.split(',') if c.strip()]
                codigos_stats['codigos_unicos'].update(codigos_list)
                
                for codigo in codigos_list:
                    codigo_frequency[codigo] = codigo_frequency.get(codigo, 0) + 1
            else:
                codigos_stats['sem_codigos'] += 1
        
        logger.info(f"  Registros com códigos: {codigos_stats['com_codigos']}")
        logger.info(f"  Registros sem códigos: {codigos_stats['sem_codigos']}")
        logger.info(f"  Códigos únicos identificados: {len(codigos_stats['codigos_unicos'])}")
        
        if codigo_frequency:
            logger.info(f"\nTop 5 códigos mais frequentes:")
            sorted_codigos = sorted(codigo_frequency.items(), key=lambda x: x[1], reverse=True)
            for codigo, freq in sorted_codigos[:5]:
                percentage = (freq / len(self.processed_data)) * 100
                logger.info(f"  {codigo}: {freq} ({percentage:.1f}%)")
    
    def _analyze_field_frequency(self) -> None:
        """Analisa frequência de campos nos dados."""
        field_counts = {}
        for record in self.processed_data:
            for field in record:
                if field not in field_counts:
                    field_counts[field] = 0
                if record[field] != '':
                    field_counts[field] += 1
        
        logger.info(f"\nTop 10 campos mais frequentes:")
        sorted_fields = sorted(field_counts.items(), key=lambda x: x[1], reverse=True)
        for field, count in sorted_fields[:10]:
            percentage = (count / len(self.processed_data)) * 100
            logger.info(f"  {field}: {count} ({percentage:.1f}%)")
    
    def process_batch(self, csv_filepath: str) -> None:
        """
        Processa lista de processos em modo batch.
        
        Args:
            csv_filepath: Caminho do arquivo CSV com números de processo
            
        Raises:
            ProcessamentoError: Em caso de falha no processamento
        """
        self.processing_stats['start_time'] = datetime.now()
        
        try:
            # Leitura de processos
            process_list = self._read_csv_processes(csv_filepath)
            self.processing_stats['total_processes'] = len(process_list)
            
            if not process_list:
                raise ProcessamentoError("Nenhum processo válido encontrado no CSV")
            
            logger.info(f"Iniciando processamento batch de {len(process_list)} processos")
            
            # Pipeline de processamento
            for numero_processo in tqdm(process_list, desc="Processando processos", unit="processo"):
                try:
                    # Etapa 1: Download via SOAP
                    logger.debug(f"Consultando processo: {numero_processo}")
                    xml_response = self._execute_soap_request(numero_processo)
                    raw_filepath = self._save_raw_response(xml_response, numero_processo)
                    self.processing_stats['successful_downloads'] += 1
                    
                    # Etapa 2: Organização de dados
                    organized_data = self._organize_raw_response(raw_filepath)
                    organized_filepath = self._save_organized_data(organized_data, numero_processo)
                    
                    # Etapa 3: Extração para agregação
                    resumo_data = self._extract_resumo_data(organized_data, organized_filepath.name)
                    if resumo_data:
                        self.processed_data.append(resumo_data)
                        self.processing_stats['successful_processing'] += 1
                    
                    # Pausa para evitar sobrecarga do servidor
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Erro no processamento de {numero_processo}: {e}")
                    self.error_files.append(numero_processo)
                    self.processing_stats['errors'] += 1
            
            # Geração de arquivo CSV agregado
            if self.processed_data:
                output_csv = self.aggregated_dir / f"dados_agregados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                self._write_aggregated_csv(str(output_csv))
            
            self.processing_stats['end_time'] = datetime.now()
            self._generate_comprehensive_report()
            
            logger.info("Processamento batch concluído com sucesso!")
            
        except Exception as e:
            self.processing_stats['end_time'] = datetime.now()
            logger.error(f"Falha no processamento batch: {e}")
            raise

def main():
    """Função principal com tratamento robusto de argumentos e exceções."""
    if len(sys.argv) != 5 or sys.argv[3] != '--csv':
        print("\nUso: python script.py {id_consultante} {codigo_privado} --csv {arquivo.csv}")
        print("\nDescrição:")
        print("  id_consultante: Identificador do consultante no sistema PROJUDI")
        print("  codigo_privado: Código privado para autenticação")
        print("  arquivo.csv: Arquivo CSV contendo números de processo (uma coluna)")
        print("\nExemplo:")
        print("  python script.py 123456 abcd1234 --csv processos.csv")
        sys.exit(1)
    
    id_consultante = sys.argv[1]
    codigo_privado = sys.argv[2]
    csv_file = sys.argv[4]
    
    # Validações iniciais
    if not os.path.isfile(csv_file):
        logger.error(f"Arquivo CSV não encontrado: {csv_file}")
        sys.exit(1)
    
    try:
        # Inicialização do processador
        processor = PROJUDIProcessor(id_consultante, codigo_privado)
        
        # Execução do processamento batch
        processor.process_batch(csv_file)
        
    except KeyboardInterrupt:
        logger.info("Processamento interrompido pelo usuário")
        sys.exit(0)
    except ProcessamentoError as e:
        logger.error(f"Erro de processamento: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erro inesperado: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()